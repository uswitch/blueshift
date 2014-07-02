(ns uswitch.blueshift.telemetry
  (:require [com.stuartsierra.component :refer (system-map Lifecycle start stop)]
            [metrics.core :refer (default-registry)]
            [clojure.tools.logging :refer (info *logger-factory* debug)]
            [clojure.tools.logging.impl :refer (get-logger)]
            [clojure.string :refer (split)]
            [uswitch.blueshift.util :refer (clear-keys)])
  (:import [java.util.concurrent TimeUnit]
           [com.codahale.metrics Slf4jReporter]))

(defrecord LogMetricsReporter [registry]
  Lifecycle
  (start [this]
    (let [reporter (.build (doto (Slf4jReporter/forRegistry registry)
                             (.outputTo (get-logger *logger-factory* *ns*))
                             (.convertRatesTo TimeUnit/SECONDS)
                             (.convertDurationsTo TimeUnit/MILLISECONDS)))]
      (info "Starting Slf4j metrics reporter")
      (.start reporter 1 TimeUnit/MINUTES)
      (assoc this :reporter reporter)))
  (stop [this]
    (when-let [reporter (:reporter this)]
      (info "Stopping Slf4j metrics reporter")
      (.stop reporter))
    (clear-keys this :reporter)))

(defn log-metrics-reporter [config registry]
  (map->LogMetricsReporter {:registry registry}))

(defn- load-reporter [config sym]
  (info "Loading reporter" sym "for registry" default-registry)
  (require (symbol (namespace sym)))
  (let [sysfn (var-get (find-var sym))]
    (sysfn config default-registry)))

(defn telemetry-system [config]
  (let [configured-reporters (-> config :telemetry :reporters)]
    (reduce (fn [system reporter]
              (assoc system (keyword (str reporter)) (load-reporter config reporter)))
            (system-map)
            configured-reporters)))
