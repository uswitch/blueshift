(ns uswitch.blueshift.system
  (:require [uswitch.blueshift.s3 :refer (s3-system)]
            [uswitch.blueshift.telemetry :refer (telemetry-system)]
            [com.stuartsierra.component :refer (system-map using Lifecycle start)]
            [clojure.core.async :refer (chan close!)])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))

(defn build-system [config]
  (system-map :s3-system (s3-system config)
              :telemetry-system (telemetry-system config)))
