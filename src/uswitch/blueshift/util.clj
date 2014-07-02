(ns uswitch.blueshift.util
  (:require [clojure.core.async :refer (close!)]))

(defn clear-keys
  "dissoc for components records. assoc's nil for the specified keys"
  [m & ks]
  (apply assoc m (interleave ks (repeat (count ks) nil))))

(defn close-channels [state & ks]
  (doseq [k ks]
    (when-let [ch (get state k)]
      (close! ch)))
  (apply clear-keys state ks))
