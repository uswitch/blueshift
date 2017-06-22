(ns uswitch.blueshift.redshift
  (:require [aws.sdk.s3 :refer (put-object delete-object)]
            [cheshire.core :refer (generate-string)]
            [clojure.tools.logging :refer (info error debug)]
            [clojure.string :as s]
            [com.stuartsierra.component :refer (system-map Lifecycle using)]
            [clojure.core.async :refer (chan <!! >!! close! thread timeout alts!!)]
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

(def open-connections (counter [(str *ns*) "redshift-connections" "open-connections"]))

(defmacro with-connection [jdbc-url & body]
  `(binding [*current-connection* (connection ~jdbc-url)]
     (inc! open-connections)
     (try (let [res# ~@body]
            (debug "COMMIT")
            (.commit *current-connection*)
            (mark! redshift-import-commits)
            res#)
          (catch SQLException e#
            (error e# "ROLLBACK")
            (mark! redshift-import-rollbacks)
            (.rollback *current-connection*)
            (throw e#))
          (finally
            (dec! open-connections)
            (when-not (.isClosed *current-connection*)
              (.close *current-connection*))))))


(defn create-staging-table-stmt [target-table staging-table]
  (prepare-statement (format "CREATE TEMPORARY TABLE %s (LIKE %s INCLUDING DEFAULTS)"
                             staging-table
                             target-table)))

(defn copy-from-s3-stmt [table manifest-url {:keys [access-key secret-key] :as creds} {:keys [columns options] :as table-manifest}]
  (prepare-statement (format "COPY %s (%s) FROM '%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' %s manifest"
                             table
                             (s/join "," columns)
                             manifest-url
                             access-key
                             secret-key
                             (s/join " " options))))

(defn truncate-table-stmt [target-table]
  (prepare-statement (format "truncate table %s" target-table)))


(defn delete-in-query [target-table staging-table key]
  (format "DELETE FROM %s WHERE %s IN (SELECT %s FROM %s)" target-table key key staging-table))

(defn delete-join-query
  [target-table staging-table keys]
  (let [where (s/join " AND " (for [pk keys] (str target-table "." pk "=" staging-table "." pk)))]
    (format "DELETE FROM %s USING %s WHERE %s" target-table staging-table where)))

(defn delete-target-query
  "Attempts to optimise delete strategy based on keys arity. With single primary keys
   its significantly faster to delete."
  [target-table staging-table keys]
  (cond (= 1 (count keys)) (delete-in-query target-table staging-table (first keys))
        :default           (delete-join-query target-table staging-table keys)))

(defn delete-target-stmt
  "Deletes rows, with the same primary key value(s), from target-table that will be
   overwritten by values in staging-table."
  [target-table staging-table keys]
  (prepare-statement (delete-target-query target-table staging-table keys)))

(defn staging-select-statement [{:keys [staging-select] :as table-manifest} staging-table]
  (cond
   (string? staging-select)     (s/replace staging-select #"\{\{table\}\}" staging-table)
   (= :distinct staging-select) (format "SELECT DISTINCT * FROM %s" staging-table)
   :default                     (format "SELECT * FROM %s" staging-table)))

(defn insert-from-staging-stmt [target-table staging-table table-manifest]
  (let [select-statement (staging-select-statement table-manifest staging-table)]
    (prepare-statement (format "INSERT INTO %s %s" target-table select-statement))))

(defn append-from-staging-stmt [target-table staging-table keys]
  (let [join-columns (s/join " AND " (map #(str "s." % " = t." %) keys))
        where-clauses (s/join " AND " (map #(str "t." % " IS NULL") keys))]
    (prepare-statement (format "INSERT INTO %s SELECT s.* FROM %s s LEFT JOIN %s t ON %s WHERE %s"
      target-table staging-table target-table join-columns where-clauses))))

(defn drop-table-stmt [table]
  (prepare-statement (format "DROP TABLE %s" table)))

(defn- aws-censor
  [s]
  (-> s
      (clojure.string/replace #"aws_access_key_id=[^;]*" "aws_access_key_id=***")
      (clojure.string/replace #"aws_secret_access_key=[^;]*" "aws_secret_access_key=***")))

(def executing-statements (counter [(str *ns*) "redshift-connections" "executing-statements"]))

(defn- execute*
  "Will return a map with error details if the statement fails"
  [statement millis]
  (try
    (inc! executing-statements)
    (.execute statement)
    (dec! executing-statements)
    nil
    (catch SQLException e
      (dec! executing-statements)
      (error "error executing statement: " (.toString statement))
      {:cause     :sql-exception
       :statement (.toString statement)
       :message   (.getMessage e)})))

(def timeouts (meter [(str *ns*) "redshift-connections" "statement-timeouts"]))

(defn execute
  "Executes statements in the order specified. Will throw an exception if the statement
   fails or the timeout is triggered."
  [{:keys [timeout-millis] :or {timeout-millis (* 1000 60 5)}} & statements]
  (loop [statements statements]
    (when-let [statement (first statements)]
      (let [result-ch (thread (execute* statement timeout-millis))
            timeout-ch (timeout timeout-millis)
            [v ch] (alts!! [result-ch timeout-ch])]
        (cond (and (= ch result-ch)
                   (not (nil? v)))  (throw (ex-info "error during execute" v))
              (= ch timeout-ch) (do (println "timeout during statement,
canceling")
                                    (mark! timeouts)
                                    (.cancel statement)
                                    (throw (ex-info "timeout during execution"
                                                    {:cause     :timeout
                                                     :statement (.toString statement)
                                                     :millis    timeout-millis})))
              :else (recur (rest statements)))))))

(defn merge-table [credentials redshift-manifest-url {:keys [table jdbc-url pk-columns strategy execute-opts] :as table-manifest}]
  (let [staging-table (str table "_staging")]
    (mark! redshift-imports)
    (with-connection jdbc-url
      (execute execute-opts
               (create-staging-table-stmt table staging-table)
               (copy-from-s3-stmt staging-table redshift-manifest-url credentials table-manifest)
               (delete-target-stmt table staging-table pk-columns)
               (insert-from-staging-stmt table staging-table table-manifest)
               (drop-table-stmt staging-table)))))

(defn replace-table [credentials redshift-manifest-url {:keys [table jdbc-url pk-columns strategy execute-opts] :as table-manifest}]
  (mark! redshift-imports)
  (with-connection jdbc-url
    (execute execute-opts
             (truncate-table-stmt table)
             (copy-from-s3-stmt table redshift-manifest-url credentials table-manifest))))

(defn append-table [credentials redshift-manifest-url {:keys [table jdbc-url pk-columns strategy execute-opts] :as table-manifest}]
  (let [staging-table (str table "_staging")]
    (mark! redshift-imports)
    (with-connection jdbc-url
      (execute execute-opts
               (create-staging-table-stmt table staging-table)
               (copy-from-s3-stmt staging-table redshift-manifest-url credentials table-manifest)
               (append-from-staging-stmt table staging-table pk-columns)
               (drop-table-stmt staging-table)))))

(defn load-table [credentials redshift-manifest-url {strategy :strategy :as table-manifest}]
  (case (keyword strategy)
    :merge (merge-table credentials redshift-manifest-url table-manifest)
    :replace (replace-table credentials redshift-manifest-url table-manifest)
    :append (append-table credentials redshift-manifest-url table-manifest)))
