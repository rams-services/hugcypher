# hugcypher
A clojure library that offers similar functionality to hugsql but geared towards the Cypher Query Language. Currently supports neo4j and arcadedb.

# Getting Started
```clojure
(ns db
    (:require [hugcypher.core :as hughcypher]
              [hugcypher.cypher as cypher]
              [clojure.java.io :as io]))

;; This will take all queries from the test.cypher file and bind them to the current ns
(hugcypher/def-db-fns (io/file "test.cypher"))

;; Each db function takes the following arguments: type, connection, and the params map
;; The type can be either :neo4j or :arcadedb-http

;; setting up a connection
(def conn (cypher/connect :neo4j {:url "bolt://localhost:7678"
                                  :username "neo4j"
                                  :password "neo4j"}))
```

```clojure
(ns example
    (:require [db :as db :refer [conn]]))                                  
                                  
;; Get user query has a user-id param
(db/get-user :neo4j conn {:user-id 1}) ;; => user will be returned

;; This's just a small example of one of the things you can do using hugcypher
```

# Version Matrix
| hughcypher | Clojure | Java   | Neo4j Java Driver | Neo4j |
|------------|---------|--------|-------------------|-------|
| 0.1.3      | 1.11.1  | 17.x.x | 5.x.x             | 5.x   |
| 0.1.2      | 1.10.1  | 11.x.x | 1.5.x             | 3.5   |

# Acknowledgements
hugcypher has been inspired by the work of the following project(s):

[hugsql](https://github.com/layerware/hugsql)
