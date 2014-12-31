(ns user
  (:require [demo.core :refer (build-system main)]
            [clojure.tools.namespace.repl :refer (refresh)]
            [com.stuartsierra.component :as component]))

(def system nil)

(defn jdbc-url []
  (or (get (System/getenv) "JDBC_URL")
      (clojure.string/trim (slurp "jdbc-url.txt"))))

(defn init []
  (alter-var-root #'system
                  (constantly (main (read-string (slurp "../etc/config.edn"))
                                    (jdbc-url)))))

(defn start []
  (alter-var-root #'system component/start))

(defn stop []
  (alter-var-root #'system (fn [s] (when s (component/stop s)))))

(defn go []
  (init)
  (start))

(defn reset []
  (stop)
  (refresh :after 'user/go))
