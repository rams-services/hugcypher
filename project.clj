(defproject hugneo4j "0.0.8"
  :description "HugNeo4j offers a similar experience to the excellent HugSql library but for neo4j. As a matter of fact this library copies some very important parts from HugSql"
  :url "https://github.com/rams-services/hugneo4j"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [clojurewerkz/neocons "3.2.0"]
                 [selmer "1.12.18"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.clojure/tools.reader "1.3.2"]]
  :profiles {:dev
             {:plugins [[lein-auto "0.1.2"]]
              :global-vars {*warn-on-reflection* false
                            *assert* false}}
             :dependencies [[org.clojure/clojure "1.10.1"]]}
  :aliases {"test-all" ["with-profile" "dev" "test"]})
