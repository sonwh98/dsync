(defproject com.kaicode/dsync "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha16"]
                 [org.clojure/clojurescript "1.9.562"]
                 [org.clojure/tools.logging "0.3.1"]
                 
                 [mount "0.1.10"]
                 ;;[com.datomic/datomic-pro "0.9.5407" :exclusions [org.slf4j/log4j-over-slf4j org.slf4j/slf4j-nop joda-time com.google.guava/guava]]
                 [datomic-schema "1.3.0"]
                 [datascript "0.15.4"]
                 [com.kaicode/wocket "0.1.3-SNAPSHOT"]
                 [com.kaicode/mercury "0.1.2-SNAPSHOT"]]
  :target-path "target/%s")
