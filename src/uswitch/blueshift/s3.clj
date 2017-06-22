(ns uswitch.blueshift.s3
  (:require [com.stuartsierra.component :refer (Lifecycle system-map using start stop)]
            [clojure.tools.logging :refer (info error warn debug errorf)]
            [aws.sdk.s3 :refer (list-objects get-object delete-object)]
            [clojure.set :refer (difference)]
            [clojure.core.async :refer (go-loop thread put! chan >!! <!! >! <! alts!! timeout close!)]
            [clojure.edn :as edn]
            [uswitch.blueshift.util :refer (close-channels clear-keys)]
            [schema.core :as s]
            [metrics.counters :refer (counter inc! dec!)]
            [metrics.timers :refer (timer time!)]
            [uswitch.blueshift.redshift :as redshift])
  (:import [java.io PushbackReader InputStreamReader]
           [org.apache.http.conn ConnectionPoolTimeoutException]))

(defrecord Manifest [table pk-columns columns jdbc-url options data-pattern strategy staging-select])

(def ManifestSchema {:table          s/Str
                     :pk-columns     [s/Str]
                     :columns        [s/Str]
                     :jdbc-url       s/Str
                     :strategy       s/Str
                     :options        s/Any
                     :staging-select (s/maybe (s/either s/Str s/Keyword))
                     :data-pattern   s/Regex})

(defn validate [manifest]
  (when-let [error-info (s/check ManifestSchema manifest)]
    (throw (ex-info "Invalid manifest. Check map for more details." error-info))))

(defn listing
  [credentials bucket & opts]
  (let [options (apply hash-map opts)]
    (loop [marker   nil
           results  nil]
      (let [{:keys [next-marker truncated? objects]}
            (list-objects credentials bucket (assoc options :marker marker))]
        (if (not truncated?)
          (concat results objects)
          (recur next-marker (concat results objects)))))))

(defn files [credentials bucket directory]
  (listing credentials bucket :prefix directory))

(defn directories
  ([credentials bucket]
     (:common-prefixes (list-objects credentials bucket {:delimiter "/"})))
  ([credentials bucket path]
     {:pre [(.endsWith path "/")]}
     (:common-prefixes (list-objects credentials bucket {:delimiter "/" :prefix path}))))

(defn leaf-directories
  [credentials bucket]
  (loop [work (directories credentials bucket)
         result nil]
    (if (seq work)
      (let [sub-dirs (directories credentials bucket (first work))]
        (recur (concat (rest work) sub-dirs)
               (if (seq sub-dirs)
                 result
                 (cons (first work) result))))
      result)))

(defn read-edn [stream]
  (edn/read (PushbackReader. (InputStreamReader. stream))))

(defn assoc-if-nil [record key value]
  (if (nil? (key record))
    (assoc record key value)
    record))

(defn manifest [credentials bucket files]
  (letfn [(manifest? [{:keys [key]}]
            (re-matches #".*manifest\.edn$" key))]
    (when-let [manifest-file-key (:key (first (filter manifest? files)))]
      (with-open [content (:content (get-object credentials bucket manifest-file-key))]
        (-> (read-edn content)
            (map->Manifest)
            (assoc-if-nil :strategy "merge")
            (update-in [:data-pattern] re-pattern))))))

(defn- step-scan
  [credentials bucket directory]
  (try
    (let [fs (files credentials bucket directory)]
      (if-let [manifest (manifest credentials bucket fs)]
        (do
          (validate manifest)
          (let [data-files  (filter (fn [{:keys [key]}]
                                      (re-matches (:data-pattern manifest) key))
                                    fs)]
            (if (seq data-files)
              (do
                (info "Watcher triggering import" (:table manifest))
                (debug "Triggering load:" load)
                {:state :load, :table-manifest manifest, :files (map :key data-files)})
              {:state :scan, :pause? true})))
        {:state :scan, :pause? true}))
    (catch clojure.lang.ExceptionInfo e
      (error e "Error with manifest file")
      {:state :scan, :pause? true})
    (catch ConnectionPoolTimeoutException e
      (warn e "Connection timed out. Will re-try.")
      {:state :scan, :pause? true})
    (catch Exception e
      (error e "Failed reading content of" (str bucket "/" directory))
      {:state :scan, :pause? true})))

(def importing-files (counter [(str *ns*) "importing-files" "files"]))
(def import-timer (timer [(str *ns*) "importing-files" "time"]))

(defn- step-load
  [credentials bucket table-manifest files]
  (let [redshift-manifest  (redshift/manifest bucket files)
        {:keys [key url]}  (redshift/put-manifest credentials bucket redshift-manifest)]
    (info "Importing" (count files) "data files to table" (:table table-manifest) "from manifest" url)
    (debug "Importing Redshift Manifest" redshift-manifest)
    (inc! importing-files (count files))
    (try (time! import-timer
                (redshift/load-table credentials url table-manifest))
         (info "Successfully imported" (count files) "files")
         (delete-object credentials bucket key)
         (dec! importing-files (count files))
         {:state :delete
          :files files}
         (catch java.sql.SQLException e
           (error e "Error loading into" (:table table-manifest))
           (error (:table table-manifest) "Redshift manifest content:" redshift-manifest)
           (delete-object credentials bucket key)
           (dec! importing-files (count files))
           {:state :scan
            :pause? true})
         (catch Exception e
           (if-let [m (ex-data e)]
             (error e "Failed to load files to table" (:table table-manifest) ": " (pr-str m))
             (error e "Failed to load files to table" (:table table-manifest) "from manifest" url))
           {:state :scan :pause? true}))))

(defn- step-delete
  [credentials bucket files]
  (do
    (doseq [key files]
      (info "Deleting" (str "s3://" bucket "/" key))
      (try
        (delete-object credentials bucket key)
        (catch Exception e
          (warn "Couldn't delete" key "  - ignoring"))))
    {:state :scan, :pause? true}))

(defn- progress
  [{:keys [state] :as world}
   {:keys [credentials bucket directory] :as configuration}]
  (case state
    :scan   (step-scan   credentials bucket directory )
    :load   (step-load   credentials bucket           (:table-manifest world) (:files world))
    :delete (step-delete credentials bucket           (:files world))))

(defrecord KeyWatcher [credentials bucket directory
                       poll-interval-seconds
                       poll-interval-random-seconds]
  Lifecycle
  (start [this]
    (info "Starting KeyWatcher for" (str bucket "/" directory) "polling every" poll-interval-seconds "seconds")
    (let [control-ch    (chan)
          configuration {:credentials credentials :bucket bucket :directory directory}]
      (thread
       (loop [timer (timeout (*
                              (+ poll-interval-seconds
                                 (int (* (rand) (float poll-interval-random-seconds))))
                              1000))
              world {:state :scan}]
         (let [next-world (progress world configuration)]
           (if (:pause? next-world)
             (let [[_ c] (alts!! [control-ch timer])]
               (when (not= c control-ch)
                 (let [t (*
                          (+ poll-interval-seconds
                             (int (* (rand) (float poll-interval-random-seconds))))
                          1000)]
                   (recur (timeout t) next-world))))
             (recur timer next-world)))))
      (assoc this :watcher-control-ch control-ch)))
  (stop [this]
    (info "Stopping KeyWatcher for" (str bucket "/" directory))
    (close-channels this :watcher-control-ch)))


(defn spawn-key-watcher! [credentials bucket directory
                          poll-interval-seconds poll-interval-random-seconds]
  (start (KeyWatcher. credentials bucket directory
                      poll-interval-seconds
                      poll-interval-random-seconds)))

(def directories-watched (counter [(str *ns*) "directories-watched" "directories"]))

(defrecord KeyWatcherSpawner [bucket-watcher
                              poll-interval-seconds
                              poll-interval-random-seconds]
  Lifecycle
  (start [this]
    (info "Starting KeyWatcherSpawner")
    (let [{:keys [new-directories-ch bucket credentials]} bucket-watcher
          watchers (atom nil)]
      (go-loop [dirs (<! new-directories-ch)]
        (when dirs
          (doseq [dir dirs]
            (swap! watchers conj (spawn-key-watcher!
                                  credentials bucket dir
                                  poll-interval-seconds
                                  poll-interval-random-seconds))
            (inc! directories-watched))
          (recur (<! new-directories-ch))))
      (assoc this :watchers watchers)))
  (stop [this]
    (info "Stopping KeyWatcherSpawner")
    (when-let [watchers (:watchers this)]
      (info "Stopping" (count @watchers) "watchers")
      (doseq [watcher @watchers]
        (stop watcher)
        (dec! directories-watched)))
    (clear-keys this :watchers)))

(defn key-watcher-spawner [config]
  (map->KeyWatcherSpawner
   {:poll-interval-seconds (-> config :s3 :poll-interval :seconds)
    :poll-interval-random-seconds (or (-> config :s3 :poll-interval :random-seconds) 0)}))

(defn matching-directories [credentials bucket key-pattern]
  (try (->> (leaf-directories credentials bucket)
            (filter #(re-matches key-pattern %))
            (set))
       (catch Exception e
         (errorf e "Error checking for matching object keys in \"%s\"" bucket)
         #{})))

(defrecord BucketWatcher [credentials bucket key-pattern poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting BucketWatcher. Polling" bucket "every" poll-interval-seconds "seconds for keys matching" key-pattern)
    (let [new-directories-ch (chan)
          control-ch         (chan)]
      (thread
        (loop [dirs nil]
          (let [available-dirs (matching-directories credentials bucket key-pattern)
                new-dirs       (difference available-dirs dirs)]
            (when (seq new-dirs)
              (info "New directories:" new-dirs "spawning" (count new-dirs) "watchers")
              (>!! new-directories-ch new-dirs))
            (let [[v c] (alts!! [(timeout (* 1000 poll-interval-seconds)) control-ch])]
              (when-not (= c control-ch)
                (recur available-dirs))))))
      (assoc this :control-ch control-ch :new-directories-ch new-directories-ch)))
  (stop [this]
    (info "Stopping BucketWatcher")
    (close-channels this :control-ch :new-directories-ch)))

(defn bucket-watcher
  "Creates a process watching for objects in S3 buckets."
  [config]
  (map->BucketWatcher {:credentials (-> config :s3 :credentials)
                       :bucket (-> config :s3 :bucket)
                       :poll-interval-seconds (-> config :s3 :poll-interval :seconds)
                       :key-pattern (or (re-pattern (-> config :s3 :key-pattern))
                                        #".*")}))

(defrecord PrintSink [prefix chan-k component]
  Lifecycle
  (start [this]
    (let [ch (get component chan-k)]
      (go-loop [msg (<! ch)]
        (when msg
          (info prefix msg)
          (recur (<! ch)))))
    this)
  (stop [this]
    this))

(defn print-sink
  [prefix chan-k]
  (map->PrintSink {:prefix prefix :chan-k chan-k}))

(defn s3-system [config]
  (system-map :bucket-watcher (bucket-watcher config)
              :key-watcher-spawner (using (key-watcher-spawner config)
                                          [:bucket-watcher])))
