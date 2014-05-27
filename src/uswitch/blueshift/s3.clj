(ns uswitch.blueshift.s3
  (:require [com.stuartsierra.component :refer (Lifecycle system-map using start stop)]
            [clojure.tools.logging :refer (info error warn)]
            [aws.sdk.s3 :refer (list-objects get-object delete-object)]
            [clojure.set :refer (difference)]
            [clojure.core.async :refer (go-loop put! chan >!! >! <! alts! timeout close!)]
            [clojure.edn :as edn]
            [uswitch.blueshift.util :refer (close-channels)]
            [schema.core :as s]
            [schema.macros :as sm])
  (:import [java.io PushbackReader InputStreamReader]
           [org.apache.http.conn ConnectionPoolTimeoutException]))


(sm/defrecord Manifest [table :- s/Str
                        pk-columns :- [s/Str]
                        columns :- [s/Str]
                        jdbc-url :- s/Str
                        options :- s/Any
                        data-pattern :- s/Regex])

(defn validate [manifest]
  (when-let [error-info (s/check Manifest manifest)]
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
            (update-in [:data-pattern] re-pattern))))))

(defrecord KeyWatcher [credentials bucket directory redshift-load-ch poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting KeyWatcher for" (str bucket "/" directory) "polling every" poll-interval-seconds "seconds")
    (let [control-ch (chan)]
      (go-loop []
        (try
          (let [fs (files credentials bucket directory)]
            (when-let [manifest (manifest credentials bucket fs)]
              (validate manifest)
              (let [data-files (filter (fn [{:keys [key]}] (re-matches (:data-pattern manifest) key)) fs)]
                (when (seq data-files)
                  (info "Watcher triggering import, found import manifest:" manifest)
                  (>!! redshift-load-ch {:table-manifest manifest
                                         :files          (map :key data-files)})))))
          (catch ConnectionPoolTimeoutException e
            (warn e "Connection timed out. Will re-try in" poll-interval-seconds "seconds"))
          (catch Exception e
            (error e "Failed reading content of" (str bucket "/" directory))))
        (let [[_ c] (alts! [control-ch (timeout (* poll-interval-seconds 1000))])]
          (when (not= c control-ch)
            (recur))))
      (assoc this :watcher-control-ch control-ch)))
  (stop [this]
    (info "Stopping KeyWatcher for" (str bucket "/" directory))
    (close-channels this :watcher-control-ch)))


(defn spawn-key-watcher! [credentials bucket directory redshift-load-ch poll-interval-seconds]
  (start (KeyWatcher. credentials bucket directory redshift-load-ch poll-interval-seconds)))

(defrecord KeyWatcherSpawner [bucket-watcher redshift-load-ch poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting KeyWatcherSpawner")
    (let [{:keys [new-directories-ch bucket credentials]} bucket-watcher
          watchers (atom nil)]
      (go-loop [dirs (<! new-directories-ch)]
        (when dirs
          (doseq [dir dirs]
            (swap! watchers conj (spawn-key-watcher! credentials bucket dir redshift-load-ch poll-interval-seconds)))
          (recur (<! new-directories-ch))))
      (assoc this :watchers watchers)))
  (stop [this]
    (info "Stopping KeyWatcherSpawner")
    (when-let [watchers (:watchers this)]
      (info "Stopping" (count @watchers) "watchers")
      (doseq [watcher @watchers]
        (stop watcher)))
    (dissoc this :watchers)))

(defn key-watcher-spawner [config]
  (map->KeyWatcherSpawner {:poll-interval-seconds (-> config :s3 :poll-interval :seconds)}))

(defrecord BucketWatcher [credentials bucket poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting BucketWatcher. Polling" bucket "every" poll-interval-seconds "seconds")
    (let [new-directories-ch (chan)
          control-ch         (chan)]
      (go-loop [dirs nil]
        (let [available-dirs (set (leaf-directories credentials bucket))
              new-dirs       (difference available-dirs dirs)]
          (when (seq new-dirs)
            (info "New directories:" new-dirs "spawning" (count new-dirs) "watchers")
            (>! new-directories-ch new-dirs))
          (let [[v c] (alts! [(timeout (* 1000 poll-interval-seconds)) control-ch])]
            (when-not (= c control-ch)
              (recur available-dirs)))))
      (assoc this :control-ch control-ch :new-directories-ch new-directories-ch)))
  (stop [this]
    (info "Stopping BucketWatcher")
    (close-channels this :control-ch :new-directories-ch)))

(defn bucket-watcher
  "Creates a process watching for objects in S3 buckets."
  [config]
  (map->BucketWatcher {:credentials (-> config :s3 :credentials)
                       :bucket (-> config :s3 :bucket)
                       :poll-interval-seconds (-> config :s3 :poll-interval :seconds)}))


(defrecord Cleaner [credentials bucket cleaner-ch]
  Lifecycle
  (start [this]
    (info "Starting Cleaner")
    (go-loop []
      (when-let [m (<! cleaner-ch)]
        (doseq [key (:files m)]
          (info "Deleting" (str "s3://" bucket "/" key))
          (delete-object credentials bucket key))
        (recur)))
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
