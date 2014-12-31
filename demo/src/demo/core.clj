(ns demo.core
  (:require [clojure.tools.logging :refer (info error warn debug errorf)]
            [com.stuartsierra.component :refer (system-map using Lifecycle start)]
            [aws.sdk.s3 :refer (list-objects get-object delete-object put-object)]
            [clojure.core.async :refer (chan close! thread alts!! timeout)]
            [clojure.tools.cli :refer (parse-opts)])
  (:import [java.util UUID]
           [java.sql DriverManager SQLException]))

;; pgsql driver isn't loaded automatically from classpath

(Class/forName "org.postgresql.Driver")

;; SQL utils

(defn- get-connection
  [jdbc-url]
  (doto (DriverManager/getConnection jdbc-url)
    (.setAutoCommit false)))

;; Lifecycle utils

(defn clear-keys
  "dissoc for components records. assoc's nil for the specified keys"
  [m & ks]
  (apply assoc m (interleave ks (repeat (count ks) nil))))

(defn close-channels [state & ks]
  (doseq [k ks]
    (when-let [ch (get state k)]
      (close! ch)))
  (apply clear-keys state ks))

;; Checking S3 directory structure

(defn- key-pattern->prefix
  "Converts a key-pattern to a prefix by replacing any .* by /"
  [key-pattern]
  (.replaceAll key-pattern "\\.\\*" "/"))

(defn- all-s3-keys
  [cred bucket key-pattern]
  (->>
   (list-objects cred bucket
                 {:prefix (key-pattern->prefix key-pattern) :delimiter "/"})
   :objects
   (map :key)))

(defrecord S3ChangePoller [config]
  Lifecycle
  (start [this]
    (let [control-ch (chan)]
      (thread
       (loop [s3-keys #{}]
         (info "Checking S3 buckets")
         (let [new-s3-keys (set
                                (all-s3-keys
                                 (-> config :s3 :credentials)
                                 (-> config :s3 :bucket)
                                 (-> config :s3 :key-pattern key-pattern->prefix
                                     (str "folder/"))))]
               (when (not= s3-keys new-s3-keys)
                 (info "S3 bucket content changed:")
                 (info (sort new-s3-keys)))
               (let [[_ port] (alts!! [(timeout 15000) control-ch])]
                 (if (= port control-ch)
                   (info "Halting S3 change poller loop")
                   (recur new-s3-keys))))))
      (assoc this :control-ch control-ch)))
  (stop [this]
    (close-channels this :control-ch)))

;; Publishing new TSV files

(defn- publish-new-tsv!
  [cred bucket prefix]
  (let [timestamp (System/currentTimeMillis)
        key (str prefix "folder/" timestamp ".tsv")
        uuid (.toString (UUID/randomUUID))]
    (put-object cred bucket
                key
                (str uuid \tab key \tab timestamp))))

(defrecord TSVPublisher [config]
  Lifecycle
  (start [this]
    (let [control-ch (chan)]
      (thread
       (loop []
         (info "Publishing new TSV file")
         (publish-new-tsv!
          (-> config :s3 :credentials)
          (-> config :s3 :bucket)
          (-> config :s3 :key-pattern key-pattern->prefix))
         (let [[_ port] (alts!! [(timeout 20000) control-ch])]
           (if (= port control-ch)
             (info "Halting TSV publisher")
             (recur)))))
      (assoc this :control-ch control-ch)))
  (stop [this]
    (close-channels this :control-ch)))

;; Poll for largest timestamp

(defn- latest-timestamp
  [jdbc-url]
  (let [c (get-connection jdbc-url)
        s (.createStatement c)
        rs (.executeQuery s "SELECT MAX(timestamp) AS mt FROM demo")]
    (when (.next rs)
      (.getTimestamp rs "mt"))))

(defrecord LatestTimestampPoller [jdbc-url]
  Lifecycle
  (start [this]
    (let [control-ch (chan)]
      (thread
       (loop [timestamp -1]
         (info "Polling Redshift for latest timestamp")
         (let [new-timestamp (latest-timestamp jdbc-url)]
           (when (not= new-timestamp timestamp)
             (info "Reshift timestamp updated:" new-timestamp))
           (let [[_ port] (alts!! [(timeout 15000) control-ch])]
             (if (= port control-ch)
               (info "Halting Redshift timestamp poller loop")
               (recur new-timestamp))))))
      (assoc this :control-ch control-ch)))
  (stop [this]
    (close-channels this :control-ch)))

;; System

(defn build-system [config jdbc-url]
  (system-map
   :tsv-publisher (TSVPublisher. config)
   :s3-change-poller (S3ChangePoller. config)
   :latest-timestamp-poller (LatestTimestampPoller. jdbc-url)))

;; Ensuring manifest file is present

(defn- manifest-as-string
  [jdbc-url]
  (-> {:table "demo"
       :pk-columns ["uuid"]
       :columns ["uuid" "key" "timestamp"]
       :options ["DELIMITER '\t'"
                 "COMPUPDATE OFF"
                 "STATUPDATE ON"
                 "ESCAPE"
                 "TRIMBLANKS"
                 "EMPTYASNULL"
                 "BLANKSASNULL"
                 "FILLRECORD"
                 "TRUNCATECOLUMNS"
                 "ROUNDEC"
                 "MAXERROR 1000"
                 "TIMEFORMAT 'epochmillisecs'"]
       :data-pattern ".*tsv$"}
      (assoc :jdbc-url jdbc-url)
      str))

(defn- ensure-manifest!
  [config jdbc-url]
  (put-object (-> config :s3 :credentials)
              (-> config :s3 :bucket)
              (str (-> config :s3 :key-pattern key-pattern->prefix) "folder/manifest.edn")
              (manifest-as-string jdbc-url)))

;; Ensure demo table is present

(defn- drop-demo-table!
  [jdbc-url]
  (let [c (get-connection jdbc-url)
        s (.createStatement c)]
    (.executeUpdate s "DROP TABLE IF EXISTS demo")
    (.commit c)))

(defn- create-demo-table!
  [jdbc-url]
  (let [c (get-connection jdbc-url)
        s (.createStatement c)]
    (.executeUpdate s "
CREATE TABLE demo (
 uuid varchar(40) NOT NULL encode lzo,
 key varchar(128) NOT NULL encode lzo,
 timestamp datetime NOT NULL encode delta32k,

 PRIMARY KEY (uuid)
)")
    (.commit c)))

;; Main entry-point

(defn main
  [config jdbc-url]
  (info "Ensuring manifest.edn present in S3 bucket")
  (ensure-manifest! config jdbc-url)
  (info "Dropping demo table if exists")
  (drop-demo-table! jdbc-url)
  (info "Creating demo table")
  (create-demo-table! jdbc-url)
  (build-system config jdbc-url))

;; ... and from the command line

(def cli-options
  [["-c" "--config CONFIG" "Path to EDN configuration file"
    :default "../etc/config.edn"
    :validate [string?]]
   ["-j" "--jdbc-url JDBC-URL" "Path to jdbc-url txt file"
    :default "./jdbc-url.txt"
    :validate [string?]]
   ["-h" "--help"]])

(defn wait! []
  (let [s (java.util.concurrent.Semaphore. 0)]
    (.acquire s)))

(defn -main [& args]
  (let [{:keys [options summary]} (parse-opts args cli-options)]
    (when (:help options)
      (println summary)
      (System/exit 0))
    (let [{:keys [config jdbc-url]} options]
      (info "Starting demo")
      (let [system (main (read-string (slurp config))
                         (clojure.string/trim (slurp jdbc-url)))]
        (start system)
        (wait!)))))
