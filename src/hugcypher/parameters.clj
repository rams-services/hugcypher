(ns hugcypher.parameters
  (:require [clojure.string :as string]
            [clojure.walk :as walk]))

(defprotocol ValueParam
  "Protocol to convert Clojure value to Neo4j value"
  (value-param [param data options]))

(defprotocol MapSet
  "Protocol to convert Clojure object to Neo4j Set result"
  (build-set-from-map [param data options]))


(defn deep-get-vec
  "Takes a param :name and returns a vector
   suitable for get-in lookups where the
   param :name starts with the form:
     :employees.0.id
   Names must be keyword keys in hashmaps in
   param data.
   Numbers must be vector indexes in vectors
   in param data."
  [param-name]
  (let [key-fn (fn [x] (if (re-find #"^\d+$" x) (Long. ^String x) (keyword x)))
        name-space (namespace param-name)
        names (string/split (name param-name) #"\.")]
    (if name-space
      (apply vector
             (keyword name-space (name (key-fn (first names))))
             (mapv key-fn (rest names)))
      (mapv key-fn names))))

(defn- clear-param-name [param-name]
  (string/replace param-name #"[\@\-\?]" "_"))

;; Default Object implementations
(extend-type Object
  ValueParam
  (value-param [param data options]
    (let [param-name (clear-param-name
                      (if (keyword? (:name param))
                        (name (:name param))
                        (:name param)))
          value (get-in data (deep-get-vec (:name param)))]
      [(str "$`"  param-name "`")
       {param-name (if (map? value)
                     (into {}
                           (for [[k v] value]
                             [(name k) v]))
                     value)}]))
  MapSet
  (build-set-from-map [param data options]
    (let [param-object (if (keyword? (:map-object param))
                         (name (:map-object param))
                         (:map-object param))
          param-name (if (keyword? (:name param))
                       (name (:name param))
                       (:name param))]
      [(string/join ", " (doall (for [[prop val] ((:name param) data)
                                      :let [prop-name (if (keyword? prop)
                                                        (name prop)
                                                        prop)]]
                                  (str "`" param-object "`.`" prop-name "` = $`" param-name "`.`" prop-name "`"))))
       {param-name (walk/stringify-keys ((:name param) data))}])))

(defmulti apply-hugcypher-param
  "Implementations of this multimethod apply a hugcypher parameter
   for a specified parameter type."
  (fn [param data options] (:type param)))

(defmethod apply-hugcypher-param :v  [param data options] (value-param param data options))
(defmethod apply-hugcypher-param :value [param data options] (value-param param data options))
(defmethod apply-hugcypher-param :m [param data options] (build-set-from-map param data options))
(defmethod apply-hugcypher-param :map [param data options] (build-set-from-map param data options))
