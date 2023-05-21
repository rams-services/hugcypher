(defproject net.clojars.rams-services/hugcypher "0.1.2"
  :description "HugCypher offers a similar experience to the excellent HugSql library but for cypher. As a matter of fact this library copies some very important parts from HugSql. It extends the original hugneo4j by providing support for both neo4j and arcadedb"
  :url "https://github.com/rams-services/hugcypher"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :scm {:dir ".."}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [clojurewerkz/neocons "3.2.0"]
                 [selmer "1.12.18"]
                 [clj-http "3.12.3"]
                 [cheshire/cheshire "5.10.1"]
                 [org.clojure/tools.reader "1.3.6"]
                 [org.clojure/tools.logging "0.4.1"]]
  :profiles {:dev
             {:plugins [[lein-auto "0.1.2"]]
              :global-vars {*warn-on-reflection* false
                            *assert* false}}
             :dependencies [[org.clojure/clojure "1.10.1"]]}
  :aliases {"test-all" ["with-profile" "dev" "test"]})
