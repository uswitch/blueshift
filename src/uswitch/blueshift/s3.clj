(ns uswitch.blueshift.s3
  (:require [com.stuartsierra.component :refer (Lifecycle system-map using start stop)]
            [clojure.tools.logging :refer (info error)]
            [aws.sdk.s3 :refer (list-objects get-object)]
            [clojure.set :refer (difference)]
            [clojure.core.async :refer (go-loop chan >! <! alts! timeout close!)]
            [clojure.edn :as edn])
  (:import [java.io PushbackReader InputStreamReader]))

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

(defn close-channels [state & ks]
  (doseq [k ks]
    (when-let [ch (get state k)]
      (close! ch)))
  (apply dissoc state ks))

(defn read-edn [stream]
  (edn/read (PushbackReader. (InputStreamReader. stream))))

(defn manifest [credentials bucket files]
  (letfn [(manifest? [{:keys [key]}]
            (re-matches #".*manifest\.edn$" key))]
    (when-let [manifest-file-key (:key (first (filter manifest? files)))]
      (read-edn (:content (get-object credentials bucket manifest-file-key))))))

(defrecord Watcher [credentials bucket directory]
  Lifecycle
  (start [this]
    (info "Starting Watcher for" (str bucket "/" directory))
    (go-loop []
      (let [fs (files credentials bucket directory)]
        (when-let [manifest (manifest credentials bucket fs)]
          (let [pattern    (re-pattern (:data-pattern manifest))
                data-files (filter (fn [{:keys [key]}] (re-matches pattern key)) fs)]
            (info "Watcher found import manifest:" manifest)
            (info "Importing:" (map :key data-files))))))
    this)
  (stop [this]
    (info "Stopping Watcher for" (str bucket "/" directory))
    this))


(defn spawn-watcher! [credentials bucket directory]
  (doto (Watcher. credentials bucket directory)
    (start)))

(defrecord Spawner [poller]
  Lifecycle
  (start [this]
    (info "Starting Spawner")
    (let [ch (:new-directories-ch poller)
          bucket (:bucket poller)
          watchers (atom nil)]
      (go-loop [dirs (<! ch)]
        (doseq [dir dirs]
          (swap! watchers conj (spawn-watcher! (:credentials poller) (:bucket poller) dir)))
        (recur (<! ch)))
      (assoc this :watchers watchers)))
  (stop [this]
    (info "Stopping Spawner")
    (when-let [watchers (:watchers this)]
      (doseq [watcher @watchers] (stop watcher)))
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
            (info "New directories:" new-dirs "spawning watchers")
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
  (system-map :poller (poller config)
              :spawner (using (spawner)
                              [:poller])))
