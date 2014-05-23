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
      (-> (read-edn (:content (get-object credentials bucket manifest-file-key)))
          (map->Manifest)
          (update-in [:data-pattern] re-pattern)))))

(defrecord Watcher [credentials bucket directory redshift-load-ch]
  Lifecycle
  (start [this]
    (info "Starting Watcher for" (str bucket "/" directory))
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
            (warn e "Connection timed out. Will re-try in 60 seconds."))
          (catch Exception e
            (error e "Failed reading content of" (str bucket "/" directory))))
        (let [[_ c] (alts! [control-ch (timeout (* 60 1000))])]
          (when (not= c control-ch)
            (recur))))
      (assoc this :watcher-control-ch control-ch)))
  (stop [this]
    (info "Stopping Watcher for" (str bucket "/" directory))
    (close-channels this :watcher-control-ch)))


(defn spawn-watcher! [credentials bucket directory redshift-load-ch]
  (start (Watcher. credentials bucket directory redshift-load-ch)))

(defrecord Spawner [poller redshift-load-ch]
  Lifecycle
  (start [this]
    (info "Starting Spawner")
    (let [ch (:new-directories-ch poller)
          bucket (:bucket poller)
          watchers (atom nil)]
      (go-loop [dirs (<! ch)]
        (when dirs
          (doseq [dir dirs]
            (swap! watchers conj (spawn-watcher! (:credentials poller) (:bucket poller) dir redshift-load-ch)))
          (recur (<! ch))))
      (assoc this :watchers watchers)))
  (stop [this]
    (info "Stopping Spawner")
    (when-let [watchers (:watchers this)]
      (info "Stopping" (count @watchers) "watchers")
      (doseq [watcher @watchers]
        (stop watcher)))
    (dissoc this :watchers)))

(defn spawner []
  (map->Spawner {}))

(defrecord Poller [credentials bucket poll-interval-seconds]
  Lifecycle
  (start [this]
    (info "Starting S3 Poller. Polling" bucket "every" poll-interval-seconds "seconds")
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
    (info "Stopping S3 Poller")
    (close-channels this :control-ch :new-directories-ch)))

(defn poller
  "Creates a process watching for objects in S3 buckets."
  [config]
  (map->Poller {:credentials (-> config :s3 :credentials)
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
              :poller (poller config)
              :spawner (using (spawner)
                              [:poller :redshift-load-ch])))
