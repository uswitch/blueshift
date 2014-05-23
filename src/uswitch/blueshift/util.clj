(ns uswitch.blueshift.util
  (:require [clojure.core.async :refer (close!)]
            [clojure.tools.logging :refer (debug)]))

(defn close-channels [state & ks]
  (doseq [k ks]
    (when-let [ch (get state k)]
      (close! ch)))
  (apply dissoc state ks))
