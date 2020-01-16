(ns hugneo4j.parameters
  (:require [clojure.string :as string]))

(defprotocol ValueParam
  "Protocol to convert Clojure value to Neo4j value"
  (value-param [param data options]))


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

;; Default Object implementations
(extend-type Object
  ValueParam
  (value-param [param data options]
    (let [param-name (if (keyword? (:name param))
                       (name (:name param))
                       (:name param))]
      [(str "$`"  param-name "`")
       {param-name (get-in data (deep-get-vec (:name param)))}])))

(defmulti apply-hugneo4j-param
  "Implementations of this multimethod apply a hugneo4j parameter
   for a specified parameter type."
  (fn [param data options] (:type param)))

(defmethod apply-hugneo4j-param :v  [param data options] (value-param param data options))
(defmethod apply-hugneo4j-param :value [param data options] (value-param param data options))
