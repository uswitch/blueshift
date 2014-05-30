(ns uswitch.blueshift.redshift
  (:require [aws.sdk.s3 :refer (put-object delete-object)]
            [cheshire.core :refer (generate-string)]
            [clojure.tools.logging :refer (info error debug)]
            [clojure.string :as s]
            [com.stuartsierra.component :refer (system-map Lifecycle using)]
            [clojure.core.async :refer (chan <! >! close! go-loop)]
            [uswitch.blueshift.util :refer (close-channels)]
            [metrics.meters :refer (mark! meter)]
            [metrics.counters :refer (inc! dec! counter)]
            [metrics.timers :refer (timer time!)])
  (:import [java.util UUID]
           [java.sql DriverManager SQLException]))


(defn manifest [bucket files]
  {:entries (for [f files] {:url (str "s3://" bucket "/" f)
                            :mandatory true})})

(defn put-manifest
  "Uploads the manifest to S3 as JSON, returns the URL to the uploaded object.
   Manifest should be generated with uswitch.blueshift.redshift/manifest."
  [credentials bucket manifest]
  (let [file-name (str (UUID/randomUUID) ".manifest")
        s3-url (str "s3://" bucket "/" file-name)]
    (put-object credentials bucket file-name (generate-string manifest))
    {:key file-name
     :url s3-url}))

(def redshift-imports (meter [(str *ns*) "redshift-imports" "imports"]))
(def redshift-import-rollbacks (meter [(str *ns*) "redshift-imports" "rollbacks"]))
(def redshift-import-commits (meter [(str *ns*) "redshift-imports" "commits"]))

;; pgsql driver isn't loaded automatically from classpath
(Class/forName "org.postgresql.Driver")

(defn connection [jdbc-url]
  (doto (DriverManager/getConnection jdbc-url)
    (.setAutoCommit false)))

(def ^{:dynamic true} *current-connection* nil)

(defn prepare-statement
  [sql]
  (.prepareStatement *current-connection* sql))

(defmacro with-connection [jdbc-url & body]
  `(binding [*current-connection* (connection ~jdbc-url)]
     (try ~@body
          (debug "COMMIT")
          (.commit *current-connection*)
          (mark! redshift-import-commits)
          (catch SQLException e#
            (error e# "ROLLBACK")
            (mark! redshift-import-rollbacks)
            (.rollback *current-connection*)
            (throw e#))
          (finally
            (when-not (.isClosed *current-connection*)
              (.close *current-connection*))))))


(defn create-staging-table-stmt [target-table staging-table]
  (prepare-statement (format "CREATE TEMPORARY TABLE %s (LIKE %s INCLUDING DEFAULTS)"
                             staging-table
                             target-table)))

(defn copy-from-s3-stmt [staging-table manifest-url {:keys [access-key secret-key] :as creds} {:keys [columns options] :as table-manifest}]
  (prepare-statement (format "COPY %s (%s) FROM '%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' %s manifest"
                             staging-table
                             (s/join "," columns)
                             manifest-url
                             access-key
                             secret-key
                             (s/join " " options))))

(defn delete-target-stmt
  "Deletes rows, with the same primary key value(s), from target-table that will be
   overwritten by values in staging-table."
  [target-table staging-table keys]
  (let [where (s/join " AND " (for [pk keys]
                                (str target-table "." pk "=" staging-table "." pk)))]
    (prepare-statement (format "DELETE FROM %s USING %s WHERE %s" target-table staging-table where))))

(defn insert-from-staging-stmt [target-table staging-table]
  (prepare-statement (format "INSERT INTO %s SELECT * FROM %s" target-table staging-table)))

(defn drop-table-stmt [table]
  (prepare-statement (format "DROP TABLE %s" table)))

(defn execute [& statements]
  (doseq [statement statements]
    (debug (.toString statement))
    (.execute statement)))

(defn load-table [credentials redshift-manifest-url {:keys [table jdbc-url pk-columns] :as table-manifest}]
  (let [staging-table (str table "_staging")]
    (debug "Connecting to" jdbc-url)
    (mark! redshift-imports)
    (with-connection jdbc-url
      (execute (create-staging-table-stmt table staging-table)
               (copy-from-s3-stmt staging-table redshift-manifest-url credentials table-manifest)
               (delete-target-stmt table staging-table pk-columns)
               (insert-from-staging-stmt table staging-table)
               (drop-table-stmt staging-table)))))

(def importing-files (counter [(str *ns*) "importing-files" "files"]))
(def import-timer (timer [(str *ns*) "importing-files" "time"]))

(defrecord Loader [credentials bucket redshift-load-ch cleaner-ch]
  Lifecycle
  (start [this]
    (info "Starting Redshift Loader")
    (go-loop [m (<! redshift-load-ch)]
      (when m
        (let [{:keys [table-manifest files]} m
              redshift-manifest              (manifest bucket files)
              {:keys [key url]}              (put-manifest credentials bucket redshift-manifest)]

          (info "Importing" (count files) "data files to table" (:table table-manifest) "from manifest" url)
          (debug "Importing Redshift Manifest" redshift-manifest)
          (inc! importing-files (count files))
          (try (time! import-timer
                      (load-table credentials url table-manifest))
               (>! cleaner-ch {:files files})
               (info "Successfully imported" (count files) "files")
               (catch java.sql.SQLException e
                 (error e "Error loading into" (:table table-manifest))
                 (error (:table table-manifest) "Redshift manifest content:" redshift-manifest))
               (finally
                 (delete-object credentials bucket key)
                 (dec! importing-files (count files))))
          (info "Finished importing" url))
        (recur (<! redshift-load-ch))))
    this)
  (stop [this]
    (info "Stopping Redshift Loader")
    this))

(defn loader [config]
  (map->Loader {:credentials (-> config :s3 :credentials)
                :bucket      (-> config :s3 :bucket)}))

(defn redshift-system [config]
  (system-map :loader (using (loader config)
                             [:redshift-load-ch :cleaner-ch])))
