(ns uswitch.blueshift.system
  (:require [uswitch.blueshift.s3 :refer (s3-system)]
            [uswitch.blueshift.redshift :refer (redshift-system)]
            [com.stuartsierra.component :refer (system-map using Lifecycle)]
            [clojure.core.async :refer (chan close!)])
  (:import [clojure.core.async.impl.channels ManyToManyChannel]))


;; TODO
;; Don't depend on underlying channel type
(extend-type ManyToManyChannel
  Lifecycle
  (stop [this] (when this (close! this)))
  (start [this] this))

(defn build-system [config]
  (system-map :s3-system (using (s3-system config)
                                [:redshift-load-ch :cleaner-ch])
              :redshift-load-ch (chan 100)
              :redshift-system (using (redshift-system config)
                                      [:redshift-load-ch :cleaner-ch])
              :cleaner-ch (chan 100)))
