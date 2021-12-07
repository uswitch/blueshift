(defproject blueshift "0.1.0-SNAPSHOT"
  :description "Automate importing S3 data into Amazon Redshift"
  :url "https://github.com/uswitch/blueshift"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "1.0.0"]
                 [com.stuartsierra/component "1.0.0"]
                 [org.clojure/core.async "1.0.567"]
                 [org.clojure/tools.cli "1.0.194"]
                 ;[clj-aws-s3 "0.3.9" :exclusions [commons-logging commons-codec joda-time]] ; TODO delete
                 [amazonica "0.3.156"]
                 [joda-time "2.10.5"]
                 [commons-codec "1.14"]
                 [org.slf4j/jcl-over-slf4j "1.7.30"]
                 [cheshire "5.10.0"]
                 [postgresql "9.3-1102.jdbc41"]
                 [prismatic/schema "1.1.12"]
                 [metrics-clojure "2.10.0"]
                 [com.codahale.metrics/metrics-jvm "3.0.2"]
                                  ;; java 11 FIX
                 ;; in 11 the java xml stuff is no longer included
                 ;; https://stackoverflow.com/questions/43574426/how-to-resolve-java-lang-noclassdeffounderror-javax-xml-bind-jaxbexception-in-j
                 [javax.xml.bind/jaxb-api "2.4.0-b180830.0359"]
                 [com.sun.xml.bind/jaxb-core "2.3.0.1"]
                 [com.sun.xml.bind/jaxb-impl "2.3.2"]
                 [javax.activation/activation "1.1.1"]
                 ]
  ;; deal with java 11
  ;; https://www.deps.co/blog/how-to-upgrade-clojure-projects-to-use-java-11/
  :managed-dependencies [[org.clojure/core.rrb-vector "0.1.1"]
                         [org.flatland/ordered "1.5.7"]]

  :profiles {:dev {:dependencies [[org.slf4j/slf4j-simple "1.7.30"]
                                  [org.clojure/tools.namespace "1.0.0"]]
                   :source-paths ["./dev"]
                   :jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=debug"
                              "-Dorg.slf4j.simpleLogger.log.org.apache.http=info"
                              "-Dorg.slf4j.simpleLogger.log.com.amazonaws=info"
                              "-Dorg.slf4j.simpleLogger.log.com.codahale=debug"]}
             :uberjar {:aot [uswitch.blueshift.main]
                       :dependencies [[ch.qos.logback/logback-classic "1.2.3"]]}}
  :main uswitch.blueshift.main)
