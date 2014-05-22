(ns uswitch.blueshift.system
  (:require [uswitch.blueshift.s3 :refer (s3-system)]
            [com.stuartsierra.component :refer (system-map)]))

(defn build-system [config]
  (system-map :s3-system (s3-system config)))
