(ns uswitch.blueshift.telemetry
  (:require [com.stuartsierra.component :refer (system-map Lifecycle start)]
            [metrics.core :refer (default-registry)]
            [clojure.tools.logging :refer (info *logger-factory*)]
            [clojure.tools.logging.impl :refer (get-logger)])
  (:import [java.util.concurrent TimeUnit]
           [com.codahale.metrics Slf4jReporter]))

(defrecord MetricsReporter []
  Lifecycle
  (start [this]
    (let [reporter (.build (doto (Slf4jReporter/forRegistry default-registry)
                             (.outputTo (get-logger *logger-factory* *ns*))
                             (.convertRatesTo TimeUnit/SECONDS)
                             (.convertDurationsTo TimeUnit/MILLISECONDS)))]
      (info "Starting Slf4j metrics reporter")
      (.start reporter 5 TimeUnit/MINUTES)
      (assoc this :reporter reporter)))
  (stop [this]
    (when-let [reporter (:reporter this)]
      (info "Stopping Slf4j metrics reporter")
      (.stop reporter))
    (dissoc this :reporter)))

(defn log-metrics-reporter []
  (start (map->MetricsReporter {})))

(defn telemetry-system []
  (system-map :log-metrics-reporter (log-metrics-reporter)))
