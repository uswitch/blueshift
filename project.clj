(.mkdir (java.io.File. "./resources"))
(spit "./resources/BUILD_NUMBER" (or (System/getenv "BUILD_NUMBER") "-1"))

(defproject blueshift "0.1.0-SNAPSHOT"
  :description "Automate importing S3 data into Amazon Redshift"
  :url "https://github.com/uswitch/blueshift"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
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
                 [prismatic/schema "0.2.2"]
                 [metrics-clojure "2.0.2"]
                 [com.codahale.metrics/metrics-jvm "3.0.2"]]
  :profiles {:dev {:dependencies [[org.slf4j/slf4j-simple "1.7.7"]
                                  [org.clojure/tools.namespace "0.2.3"]]
                   :source-paths ["./dev"]
                   :jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"
                              "-Dorg.slf4j.simpleLogger.log.org.apache.http=info"
                              "-Dorg.slf4j.simpleLogger.log.com.amazonaws=info"
                              "-Dorg.slf4j.simpleLogger.log.com.codahale=debug"]
                   :resource-paths ["/Users/paul/Work/uswitch/blueshift-riemann-metrics/target/blueshift-riemann-metrics-0.1.0-SNAPSHOT-standalone.jar"]}
             :uberjar {:aot [uswitch.blueshift.main]
                       :dependencies [[ch.qos.logback/logback-classic "1.1.2"]]}}
  :main uswitch.blueshift.main)
