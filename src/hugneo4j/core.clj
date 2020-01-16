(ns hugneo4j.core
  (:require [hugneo4j.parser :as parser]
            [hugneo4j.parameters :as parameters]
            [hugneo4j.cypher :as cypher :use [connect]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.tools.reader.edn :as edn]))

(defn ^:no-doc parsed-defs-from-string
  "Given a hugneo4j Cypher string,
   parse it, and return the defs."
  [neo4j]
  (parser/parse neo4j))

(defn ^:no-doc parsed-defs-from-file
  "Given a hugneo4j Cypher file in the classpath,
   a resource, or a java.io.File, parse it, and return the defs."
  [file]
  (parser/parse
   (slurp
    (condp instance? file
      java.io.File file ; already file
      java.net.URL file ; already resource
      ;; assume resource path (on classpath)
      (if-let [f (io/resource file)]
        f
        (throw (ex-info (str "Can not read file: " file) {})))))
   {:file file}))

(defn ^:no-doc validate-parsed-def!
  "Ensure Neo4j required headers are provided
   and throw an exception if not."
  [pdef]
  (let [header (:header pdef)]
    (when-not (or (:name header) (:name- header) (:snip header) (:snip- header))
      (throw (ex-info
              (str "Missing HugNeo4j Header of :name, :name-, :snip, or :snip-\n"
                   "Found headers include: " (pr-str (vec (keys header))) "\n"
                   "Cypher: " (pr-str (:cypher pdef))) {})))
    (when (every? empty? [(:name header) (:name- header) (:snip header) (:snip- header)])
      (throw (ex-info
              (str "HugNeo4j Header :name, :name-, :snip, or :snip- not given.\n"
                   "Cypher: " (pr-str (:cypher pdef))) {})))))

(defn ^:no-doc validate-parameters!
  "Ensure Cypher template parameters match provided param-data,
   and throw an exception if mismatch."
  [cypher-template param-data]
  (let [not-found (Object.)]
    (doseq [k (map :name (filter map? cypher-template))]
      (when-not
       (not-any?
        #(= not-found %)
        (map #(get-in param-data % not-found)
             (rest (reductions
                    (fn [r x] (conj r x))
                    []
                    (parameters/deep-get-vec k)))))
        (throw (ex-info
                (str "Parameter Mismatch: "
                     k " parameter data not found.") {}))))))

(defn ^:no-doc prepare-cypher
  "Takes a cypher template (from parser) and the runtime-provided
  param data and creates the input for the query function."
  ([neo4j-template param-data options]
   (let [_ (validate-parameters! neo4j-template param-data)
         applied (map
                  #(if (string? %)
                     [%]
                     (parameters/apply-hugneo4j-param % param-data options))
                  neo4j-template)
         neo4j    (string/join "" (map first applied))
         params (apply concat (filterv seq (map rest applied)))]
     [(string/trim neo4j) params])))

(def default-cyphervec-options
  {:quoting :off
   :fn-suffix "-cyphervec"})

(def default-db-options {:quoting :off})

(defn- str->key
  [str]
  (keyword (string/replace-first str #":" "")))

(defn ^:no-doc response-sym
  [header]
  (let [nam (or (:name header) (:name- header))]
    (or
     ;;                ↓ short-hand response position
     ;; -- :name my-fn :obj :1 :dp
     (when-let [c (second nam)] (str->key c))
     ;; -- :response :list
     (when-let [c (first (:command header))] (str->key c))
     ;; default
     :obj)))

(defn ^:no-doc result-sym
  [header]
  (let [nam (or (:name header) (:name- header))]
    (keyword
     (or
      ;;                   ↓ short-hand result position
      ;; -- :name my-fn :obj :1 :dp
      (when-let [r (second (next nam))] (str->key r))
      ;; -- :result :1
      (when-let [r (first (:result header))] (str->key r))
      ;; default
      :many))))

(defn ^:no-doc debug-sym
  [header]
  (let [nam (or (:name header) (:name- header))]
    (keyword
     (or
      ;;                        ↓ short-hand result position
      ;; -- :name my-fn :obj :1 :dp
      (when (>= (count nam) 4)
        (let [r (nth nam 3)] (str->key r)))
      ;; -- :debugging :p
      (when-let [r (first (:debug header))] (str->key r))
      ;; default
      :default))))

(defn ^:no-doc audit-sym
  [header]
  (if (not (empty? (:audit header)))
    (let [audit-info (edn/read-string (get-in header [:audit 0]))]
      (if (and (:by audit-info)
               (:message audit-info)
               (or (:node audit-info)
                   (:nodes audit-info)))
        :audit
        :no-audit))
    :no-audit))

(defmulti debug-fn identity)
(defmethod debug-fn :dp [sym] `cypher/debug-and-monitor)
(defmethod debug-fn :p [sym] `cypher/monitor)
(defmethod debug-fn :d [sym] `cypher/debug)
(defmethod debug-fn :default [sym] `cypher/no-debug-needed)

(defmulti output-fn identity)
(defmethod output-fn :dp [sym] `cypher/save-monitor)
(defmethod output-fn :p [sym] `cypher/save-monitor)
(defmethod output-fn :d [sym] `cypher/no-monitor)
(defmethod output-fn :default [sym] `cypher/no-monitor)


(defmulti audit-fn identity)
(defmethod audit-fn :audit [sym] `cypher/audit)
(defmethod audit-fn :default [sym] `cypher/without-audit)

(defmulti response-fn identity)
(defmethod response-fn :list [sym] 'cypher/to-list)
(defmethod response-fn :obj [sym] 'cypher/to-obj)

(defmulti result-fn identity)
(defmethod result-fn :1 [sym] 'cypher/first-result)
(defmethod result-fn :one [sym] 'cypher/first-result)
(defmethod result-fn :* [sym] 'cypher/all-results)
(defmethod result-fn :many [sym] 'cypher/all-results)

(defn cyphervec-fn*
  "Given parsed cypher and optional options, return an
   anonymous function that returns hugneo4j format"
  ([pcypher] (cyphervec-fn* pcypher {}))
  ([pcypher options]
   (fn y
     ([] (y {} {}))
     ([param-data] (y param-data {}))
     ([param-data opts]
      (prepare-cypher pcypher param-data (merge default-cyphervec-options options opts))))))

(defn cyphervec-fn
  "Given an cypher string and optional options, return an
   anonymous function that returns hugneo4j format"
  ([cypher] (cyphervec-fn cypher {}))
  ([cypher options]
   (let [cypher-vec (:cypher (first (parser/parse cypher {:no-header true})))]
     (cyphervec-fn* cypher-vec options))))

(def snip-fn "Alias for cyphervec-fn" cyphervec-fn)

(defn cyphervec
  "Given an cypher string, optional options, and param data, return a cyphervec"
  ([cypher param-data] (cyphervec cypher {} param-data))
  ([cypher options param-data]
   (let [f (cyphervec-fn cypher options)]
     (f param-data))))

(def snip "Alias for cyphervec" cyphervec)

(defn cyphervec-fn-map
  "Hashmap of cyphervec/snip fn from a parsed def
   with the form:
   {:fn-name {:meta {:doc \"doc string\"}
              :fn <anon-db-fn>}"
  [{:keys [cypher header]} options]
  (let [private-snip (:snip- header) ;; private snippet
        public-snip (:snip header)  ;; public snippet
        private-name (:name- header) ;; private name
        public-name (:name header)  ;; public name
        fn-name (symbol
             (str (first (or private-snip public-snip private-name public-name))
                  (when (or private-name public-name)
                    (:fn-suffix (merge default-cyphervec-options options)))))
        doc (str (or (first (:doc header)) "")
                 (when (or private-name public-name) " (cyphervec)"))
        meta (merge (if-let [m (:meta header)]
                      (edn/read-string (string/join " " m))
                      {})
                    {:doc doc
                     :file (:file header)
                     :line (:line header)
                     :arglists '([] [params] [params options])}
                    (when (or private-snip private-name) {:private true})
                    (when (or private-snip public-snip) {:snip? true}))]
    {(keyword fn-name) {:meta meta
                        :fn (cyphervec-fn* cypher (assoc options :fn-name fn-name))}}))

(defn intern-cyphervec-fn
  "Intern the cyphervec fn from a parsed def"
  [pdef options]
  (let [fm (cyphervec-fn-map pdef options)
        fk (ffirst fm)]
    (intern *ns*
            (with-meta (symbol (name fk)) (-> fm fk :meta))
            (-> fm fk :fn))))

(defmacro def-cyphervec-fns
  "Given a HugNeo4j Cypher file, define the <name>-cyphervec functions in the
  current namespace.  Returns cyphervec format
  Usage:

   (def-cyphervec-fns file options?)

   where:
    - file is a string file path in your classpath,
      a resource object (java.net.URL),
      or a file object (java.io.File)
    - options (optional) hashmap:
      {:fn-suffix \"-cyphervec\"}
   
   :fn-suffix is appended to the defined function names to
   differentiate them from the functions defined by def-db-fns."
  ([file] `(def-cyphervec-fns ~file {}))
  ([file options]
   `(doseq [~'pdef (parsed-defs-from-file ~file)]
      (validate-parsed-def! ~'pdef)
      (intern-cyphervec-fn ~'pdef ~options))))

(defmacro def-cyphervec-fns-from-string
  ([s] `(def-cyphervec-fns-from-string ~s {}))
  ([s options]
   `(doseq [~'pdef (parsed-defs-from-string ~s)]
      (validate-parsed-def! ~'pdef)
      (intern-cyphervec-fn ~'pdef ~options))))

(defmacro map-of-cyphervec-fns
  "Given a HugNeo4j Cypher file, return a hashmap of database
   functions of the form:

   {:fn1-name {:meta {:doc \"doc string\"}
               :fn <fn1>}
    :fn2-name {:meta {:doc \"doc string\"
                      :private true}
               :fn <fn2>}}"
  ([file] `(map-of-cyphervec-fns ~file {}))
  ([file options]
   `(let [~'pdefs (parsed-defs-from-file ~file)]
      (doseq [~'pdef ~'pdefs]
        (validate-parsed-def! ~'pdef))
      (apply merge
             (map #(cyphervec-fn-map % ~options) ~'pdefs)))))

(defmacro map-of-cyphervec-fns-from-string
  ([s] `(map-of-cyphervec-fns-from-string ~s {}))
  ([s options]
   `(let [~'pdefs (parsed-defs-from-string ~s)]
      (doseq [~'pdef ~'pdefs]
        (validate-parsed-def! ~'pdef))
      (apply merge
             (map #(cyphervec-fn-map % ~options) ~'pdefs)))))

(defn db-fn*
  "Given parsed cypher and optionally a response, result, audit, debug and options,
  return an anonymous function that can run hugneo4j database
  query and supports hugneo4j parameter replacement"
  ([parsed-cypher] (db-fn* parsed-cypher :obj :* :default :default {}))
  ([parsed-cypher response] (db-fn* parsed-cypher response :* :default :default {}))
  ([parsed-cypher response result] (db-fn* parsed-cypher response result :default :default {}))
  ([parsed-cypher response result audit] (db-fn* parsed-cypher response result audit :default {}))
  ([parsed-cypher response result audit debug] (db-fn* parsed-cypher response result audit debug {}))
  ([parsed-cypher response result audit debug options]
   (fn y
     ([conn] (y conn {} {}))
     ([conn param-data] (y conn param-data {}))
     ([conn param-data opts]
      (let [o (merge default-db-options options opts
                     {:response response :result result
                      :audit audit :debug debug})]
        (as-> parsed-cypher var-x
          (prepare-cypher var-x param-data o)
          ((resolve (debug-fn debug)) var-x o)
          (cypher/query conn var-x o)
          ((resolve (audit-fn audit)) conn var-x o
           (:audit options)
           param-data)
          ((resolve (response-fn response)) var-x)
          ((resolve (result-fn result)) var-x)
          ((resolve (output-fn debug)) var-x o)))))))

(defn db-fn
  "Given parsed cypher and optionally a response, result, audit, debug and options,
  return an anonymous function that can run hugneo4j database
  query and supports hugneo4j parameter replacement"
  ([cypher] (db-fn cypher :obj :* :default :default {}))
  ([cypher response] (db-fn cypher response :* :default :default {}))
  ([cypher response result] (db-fn cypher response result :default :default {}))
  ([cypher response result audit] (db-fn cypher response result audit :default {}))
  ([cypher response result audit debug] (db-fn cypher response result audit debug {}))
  ([cypher response result audit debug options]
   (let [pcypher (:cypher (first (parser/parse cypher {:no-header true})))]
     (db-fn* pcypher response result audit debug options))))

(defn db-fn-map
  "Hashmap of db fn from a parsed def
   with the form:
   {:fn-name {:meta {:doc \"doc string\"}
              :fn <anon-db-fn>}"
  [{:keys [cypher header file line]} options]
  (let [privat-name (:name- header)
        fn-name (symbol (first (or (:name header) privat-name)))
        doc (or (first (:doc header)) "")
        response (response-sym header)
        result (result-sym header)
        debug (debug-sym header)
        audit (audit-sym header)
        meta (merge (if-let [m (:meta header)]
                      (edn/read-string (string/join " " m)) {})
                    {:doc doc
                     :response response
                     :result result
                     :debug debug
                     :audit audit
                     :file (:file header)
                     :line (:line header)
                     :arglists '([db]
                                 [db params]
                                 [db params options & command-options])}
                    (when privat-name {:private true}))]
    {(keyword fn-name) {:meta meta
                        :fn (db-fn* cypher response result audit
                                    debug (assoc options :fn-name fn-name
                                                 :audit (edn/read-string (get-in header [:audit 0]))))}}))

(defn intern-db-fn
  "Intern the db fn from a parsed def"
  [pdef options]
  (let [fm (db-fn-map pdef options)
        fk (ffirst fm)]
    (intern *ns*
            (with-meta (symbol (name fk)) (-> fm fk :meta))
            (-> fm fk :fn))))

(defn ^:no-doc snippet-pdef?
  [pdef]
  (or (:snip- (:header pdef)) (:snip (:header pdef))))

(defmacro def-db-fns
  "Given a HugNeo4j CYPHER file, define the database
   functions in the current namespace.

   Usage:

   (def-db-fns file options?)"
  ([file] `(def-db-fns ~file {}))
  ([file options]
   `(doseq [~'pdef (parsed-defs-from-file ~file)]
      (validate-parsed-def! ~'pdef)
      (if (snippet-pdef? ~'pdef)
        (intern-cyphervec-fn ~'pdef ~options)
        (intern-db-fn ~'pdef ~options)))))

(defmacro def-db-fns-from-string
  "Given a Hugneo4j CYPHER string, define the database
   functions in the current namespace.

   Usage:

   (def-db-fns-from-string s options?)"
  ([s] `(def-db-fns-from-string ~s {}))
  ([s options]
   `(doseq [~'pdef (parsed-defs-from-string ~s)]
      (validate-parsed-def! ~'pdef)
      (if (snippet-pdef? ~'pdef)
        (intern-cyphervec-fn ~'pdef ~options)
        (intern-db-fn ~'pdef ~options)))))

(defmacro map-of-db-fns
  "Given a Hugneo4j CYPHER file, return a hashmap of database
   functions of the form:

   {:fn1-name {:meta {:doc \"doc string\"}
               :fn <fn1>}
    :fn2-name {:meta {:doc \"doc string\"
                      :private true}
               :fn <fn2>}}

   Usage:

   (map-of-db-fns file options?)
"
  ([file] `(map-of-db-fns ~file {}))
  ([file options]
   `(let [~'pdefs (parsed-defs-from-file ~file)]
      (doseq [~'pdef ~'pdefs]
        (validate-parsed-def! ~'pdef))
      (apply merge
             (map
              #(if (snippet-pdef? %)
                 (cyphervec-fn-map % ~options)
                 (db-fn-map % ~options))
              ~'pdefs)))))

(defmacro map-of-db-fns-from-string
  "Given a Hugneo4j CYPHER string, return a hashmap of database
   functions of the form:

   {:fn1-name {:meta {:doc \"doc string\"}
               :fn <fn1>}
    :fn2-name {:meta {:doc \"doc string\"
                      :private true}
               :fn <fn2>}}

   Usage:

   (map-of-db-fns-from-string s options?)
"
  ([s] `(map-of-db-fns-from-string ~s {}))
  ([s options]
   `(let [~'pdefs (parsed-defs-from-string ~s)]
      (doseq [~'pdef ~'pdefs]
        (validate-parsed-def! ~'pdef))
      (apply merge
             (map
              #(if (snippet-pdef? %)
                 (cyphervec-fn-map % ~options)
                 (db-fn-map % ~options))
              ~'pdefs)))))

(defn db-run
  "Given a database spec/connection, cypher string,
   parameter data, and optional command, result,
   and options, run the cypher statement"
  ([db cypher] (db-run db cypher {} :obj :* :default :default {}))
  ([db cypher param-data] (db-run db cypher param-data :obj :* :default :default {}))
  ([db cypher param-data response] (db-run db cypher param-data response :* :default :default {}))
  ([db cypher param-data response result] (db-run db cypher param-data response result :default :default {}))
  ([db cypher param-data response result audit]
   (db-run db cypher param-data response result audit :default {}))
  ([db cypher param-data response result audit debug]
   (db-run db cypher param-data response result audit debug {}))
  ([db cypher param-data response result audit debug options & command-options]
   (let [f (db-fn cypher response result audit debug options)]
     (f db param-data command-options))))
