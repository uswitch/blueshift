(ns uswitch.blueshift.main
  (:require [clojure.tools.logging :refer (info error)]
            [clojure.tools.cli :refer (parse-opts)]
            [uswitch.blueshift.system :refer (build-system)]
            [com.stuartsierra.component :refer (start stop)])
  (:gen-class))

(defn register-default-exception-handler!
  [system]
  (Thread/setDefaultUncaughtExceptionHandler
   (reify Thread$UncaughtExceptionHandler
     (uncaughtException [_ thread e]
       (error "Unhanled exception in thread " (str thread))
       (error e)
       (error "Caught unhandled exception! Stopping system and terminating!")
       (stop system)
       (System/exit 2)))))

(def cli-options
  [["-c" "--config CONFIG" "Path to EDN configuration file"
    :default "./etc/config.edn"
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
    (let [{:keys [config]} options]
      (info "Starting Blueshift with configuration" config)
      (let [system (build-system (read-string (slurp config)))]
        (register-default-exception-handler! system)
        (start system)
        (wait!)))))
