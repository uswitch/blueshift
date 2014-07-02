(ns uswitch.blueshift.s3
  (:require [com.stuartsierra.component :refer (Lifecycle system-map using start stop)]
            [clojure.tools.logging :refer (info error warn debug)]
            [aws.sdk.s3 :refer (list-objects get-object delete-object)]
            [clojure.set :refer (difference)]
            [clojure.core.async :refer (go-loop thread put! chan >!! <!! >! <! alts!! timeout close!)]
            [clojure.edn :as edn]
            [uswitch.blueshift.util :refer (close-channels clear-keys)]
            [schema.core :as s]
            [metrics.counters :refer (counter inc! dec!)])
  (:import [java.io PushbackReader InputStreamReader]
           [org.apache.http.conn ConnectionPoolTimeoutException]))

(defrecord Manifest [table pk-columns columns jdbc-url options data-pattern strategy])

(defn assoc-if-nil [record key value]
  (if (nil? (key record))
    (assoc record key value)
    record
  ))

(def ManifestSchema 
  {
    :table                        s/Str
    :pk-columns                   [s/Str]
    :columns                      [s/Str]
    :jdbc-url                     s/Str
    :strategy                     s/Str
    :options                      s/Any
    :data-pattern                 s/Regex
  })

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

(defn manifest [credentials bucket files]
  (letfn [(manifest? [{:keys [key]}]
            (re-matches #".*manifest\.edn$" key))]
    (when-let [manifest-file-key (:key (first (filter manifest? files)))]
      (with-open [content (:content (get-object credentials bucket manifest-file-key))]
        (-> (read-edn content)
            (map->Manifest)
            (assoc-if-nil :strategy "merge")
            (update-in [:data-pattern] re-pattern))))))

(defn- time-since
  "Returns a function that returns the number of milliseconds since when
  this function was invoked."
  []
  (let [now (System/currentTimeMillis)]
    (fn []
      (- (System/currentTimeMillis) now))))

(defrecord KeyWatcher [credentials bucket directory redshift-load-ch poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting KeyWatcher for" (str bucket "/" directory) "polling every" poll-interval-seconds "seconds")
    (let [control-ch (chan)]
      (thread
       (loop []
         (let [elapsed-time (time-since)]
           (try
             (let [fs (files credentials bucket directory)]
               (when-let [manifest (manifest credentials bucket fs)]
                 (validate manifest)
                 (let [data-files  (filter (fn [{:keys [key]}]
                                             (re-matches (:data-pattern manifest) key))
                                           fs)
                       complete-ch (chan)
                       load        {:table-manifest manifest
                                    :files          (map :key data-files)
                                    :complete-ch    complete-ch}]
                   (when (seq data-files)
                     (info "Watcher triggering import" (:table manifest))
                     (debug "Triggering load:" load)
                     (>!! redshift-load-ch load)
                     (debug "Waiting for completion")
                     (<!! complete-ch)))))
             (catch clojure.lang.ExceptionInfo e
               (error e "Error with manifest file"))
             (catch ConnectionPoolTimeoutException e
               (warn e "Connection timed out. Will re-try in" poll-interval-seconds "seconds"))
             (catch Exception e
               (error e "Failed reading content of" (str bucket "/" directory))))
           (let [pause-millis (max 0 (- (* poll-interval-seconds 1000) (elapsed-time)))
                 [_ c] (alts!! [control-ch (timeout pause-millis)])]
             (when (not= c control-ch)
               (recur))))))
      (assoc this :watcher-control-ch control-ch)))
  (stop [this]
    (info "Stopping KeyWatcher for" (str bucket "/" directory))
    (close-channels this :watcher-control-ch)))


(defn spawn-key-watcher! [credentials bucket directory redshift-load-ch poll-interval-seconds]
  (start (KeyWatcher. credentials bucket directory redshift-load-ch poll-interval-seconds)))

(def directories-watched (counter [(str *ns*) "directories-watched" "directories"]))

(defrecord KeyWatcherSpawner [bucket-watcher redshift-load-ch poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting KeyWatcherSpawner")
    (let [{:keys [new-directories-ch bucket credentials]} bucket-watcher
          watchers (atom nil)]
      (go-loop [dirs (<! new-directories-ch)]
        (when dirs
          (doseq [dir dirs]
            (swap! watchers conj (spawn-key-watcher! credentials bucket dir redshift-load-ch poll-interval-seconds))
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
  (map->KeyWatcherSpawner {:poll-interval-seconds (-> config :s3 :poll-interval :seconds)}))

(defrecord BucketWatcher [credentials bucket key-pattern poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting BucketWatcher. Polling" bucket "every" poll-interval-seconds "seconds for keys matching" key-pattern)
    (let [new-directories-ch (chan)
          control-ch         (chan)]
      (thread
       (loop [dirs nil]
         (let [available-dirs (->> (leaf-directories credentials bucket)
                                   (filter #(re-matches key-pattern %))
                                   (set))
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

(defrecord Cleaner [credentials bucket cleaner-ch]
  Lifecycle
  (start [this]
    (info "Starting Cleaner")
    (thread
     (loop []
       (when-let [m (<!! cleaner-ch)]
         (doseq [key (:files m)]
           (info "Deleting" (str "s3://" bucket "/" key))
           (delete-object credentials bucket key))
         (recur))))
    this)
  (stop [this]
    (info "Stopping Cleaner")
    this))

(defn cleaner [config]
  (map->Cleaner {:credentials (-> config :s3 :credentials)
                 :bucket      (-> config :s3 :bucket)}))


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
  (system-map :cleaner (using (cleaner config)
                              [:cleaner-ch])
              :bucket-watcher (bucket-watcher config)
              :key-watcher-spawner (using (key-watcher-spawner config)
                                          [:bucket-watcher :redshift-load-ch])))
