(defproject demo "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/tools.logging "0.2.6"]
                 [com.stuartsierra/component "0.2.1"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [org.clojure/tools.cli "0.3.1"]
                 [clj-aws-s3 "0.3.9" :exclusions [commons-logging commons-codec joda-time]]
                 [joda-time "2.6"]
                 [commons-codec "1.3"]
                 [org.slf4j/jcl-over-slf4j "1.7.7"]
                 [cheshire "5.3.1"]
                 [postgresql "8.0-318.jdbc3"]
                 [org.slf4j/slf4j-simple "1.7.7"]
                 [org.clojure/tools.namespace "0.2.3"]]
  :jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"
             "-Dorg.slf4j.simpleLogger.log.org.apache.http=info"
             "-Dorg.slf4j.simpleLogger.log.com.amazonaws=info"
             "-Dorg.slf4j.simpleLogger.log.com.codahale=debug"]
  :profiles {:dev {:source-paths ["./dev"]}}
  :main demo.core)
