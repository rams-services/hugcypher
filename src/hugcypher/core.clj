(ns hugcypher.core
  (:require [hugcypher.parser :as parser]
            [hugcypher.parameters :as parameters]
            [hugcypher.cypher :as cypher]
            [hugcypher.expr-run]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]))

(defn ^:no-doc parsed-defs-from-string
  "Given a hugcypher Cypher string,
   parse it, and return the defs."
  [cypher]
  (parser/parse cypher))

(defn ^:no-doc parsed-defs-from-file
  "Given a hugcypher Cypher file in the classpath,
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
   {:file (condp instance? file
            java.io.File (.getName file)
            java.net.URL (.getFile file)
            file)}))

(defn ^:no-doc validate-parsed-def!
  "Ensure Cypher required headers are provided
   and throw an exception if not."
  [pdef]
  (let [header (:header pdef)]
    (when-not (or (:name header) (:name- header) (:snip header) (:snip- header))
      (throw (ex-info
              (str "Missing Hugcypher Header of :name, :name-, :snip, or :snip-\n"
                   "Found headers include: " (pr-str (vec (keys header))) "\n"
                   "Cypher: " (pr-str (:cypher pdef))) {})))
    (when (every? empty? [(:name header) (:name- header) (:snip header) (:snip- header)])
      (throw (ex-info
              (str "Hugcypher Header :name, :name-, :snip, or :snip- not given.\n"
                   "Cypher: " (pr-str (:cypher pdef))) {})))))

(defn ^:no-doc validate-parameters!
  "Ensure Cypher template parameters match provided param-data,
   and throw an exception if mismatch."
  [cypher-template param-data]
  (let [not-found (Object.)]
    (doseq [param (filter map? cypher-template)
            :let [k (:name param)
                  h? (:map-object param)]]
      (when
          (or
           (some
            #(= not-found %)
            (map #(get-in param-data % not-found)
                 (rest (reductions
                        (fn [r x] (conj r x))
                        []
                        (parameters/deep-get-vec k)))))
           (and h?
                (empty? (get-in param-data (parameters/deep-get-vec k) not-found))))
        (throw (ex-info
                (str "Parameter Mismatch: "
                     k " parameter data not found.") {}))))))


(defn ^:no-doc expr-name
  [expr]
  (str "expr-" (hash (pr-str expr))))

(defn ^:no-doc def-expr
  ([expr] (def-expr expr nil))
  ([expr require-str]
   (let [nam (keyword (expr-name expr))
         tag (reduce
              (fn [r c]
                (if (vector? c)
                  (conj r {:expr c})
                  (if-let [o (:other (last r))]
                    (conj (vec (butlast r)) (assoc (last r) :other (conj o c)))
                    (conj r {:other [c]}))))
              []
              expr)
         clj (str "(ns hugcypher.expr-run\n"
                  (when-not (string/blank? require-str)
                    (str "(:require " require-str ")"))
                  ")\n"
                  "(swap! exprs assoc " nam "(fn [params options] "
                  (string/join
                   " "
                   (filter
                    #(not (= % :cont))
                    (map (fn [x]
                           (if (:expr x)
                             (first (:expr x))
                             (pr-str (:other x))))
                         tag)))
                  "))")]
     (load-string clj))))

(defn ^:no-doc compile-exprs
  "Compile (def) all expressions in a parsed def"
  [pdef]
  (let [require-str (string/join " " (:require (:header pdef)))]
    (doseq [expr (filter vector? (:cypher pdef))]
      (def-expr expr require-str))))


(defn ^:no-doc run-expr
  "Run expression and return result.
   Example assuming cols is [\"id\"]:

   [[\"(if (seq (:cols params))\" :cont]
     {:type :i* :name :cols}
     [:cont] \"*\" [\")\" :end]]
   to:
   {:type :i* :name :cols}"
  [expr params options]
  (let [expr-fn #(get @hugcypher.expr-run/exprs (keyword (expr-name expr)))]
    (when (nil? (expr-fn)) (def-expr expr))
    (while (nil? (expr-fn)) (Thread/sleep 1))
    (let [result ((expr-fn) params options)]
      (if (string? result)
        (:cypher (first (parser/parse result {:no-header true})))
        result))))

(defn ^:no-doc expr-pass
  "Takes a cypher template (from parser) and evaluates the
  Clojure expressions resulting in returning a cypher template
  containing only cypher strings and hashmap parameters"
  [cypher-template params options]
  (loop [curr (first cypher-template)
         pile (rest cypher-template)
         rcypher []
         expr []]
    (if-not curr
      rcypher
      (cond
        (or (vector? curr) (seq expr)) ;; expr start OR already in expr
        ;; expr end found, so run
        (if (and (vector? curr) (= :end (last curr)))
          (recur (first pile) (rest pile)
                 (if-let [r (run-expr (conj expr curr) params options)]
                   (vec (concat rcypher (if (string? r) (vector r) r)))
                   rcypher) [])
          (recur (first pile) (rest pile)
                 rcypher (conj expr curr)))

        :else
        (recur (first pile) (rest pile) (conj rcypher curr) expr)))))


(defn ^:no-doc prepare-cypher
  "Takes a cypher template (from parser) and the runtime-provided
  param data and creates the input for the query function."
  ([cypher-template param-data options]
   (let [cypher-template (expr-pass cypher-template param-data options)
         _ (validate-parameters! cypher-template param-data)
         applied (map
                  #(if (string? %)
                     [%]
                     (parameters/apply-hugcypher-param % param-data options))
                  cypher-template)
         cypher    (string/join "" (map first applied))
         params (apply concat (filterv seq (map rest applied)))]
     [(string/trim cypher) params])))

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
      ;;                      ↓ short-hand result position
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
(defmethod response-fn :list [sym] `cypher/to-list)
(defmethod response-fn :obj [sym] `cypher/to-obj)
(defmethod response-fn :bool [sym] `cypher/to-bool)
(defmethod response-fn :default [sym] `cypher/to-obj)

(defmulti result-fn identity)
(defmethod result-fn :1 [sym] `cypher/first-result)
(defmethod result-fn :one [sym] `cypher/first-result)
(defmethod result-fn :* [sym] `cypher/all-results)
(defmethod result-fn :many [sym] `cypher/all-results)
(defmethod result-fn :default [sym] `cypher/all-results)

(defn cyphervec-fn*
  "Given parsed cypher and optional options, return an
   anonymous function that returns hugcypher format"
  ([pcypher] (cyphervec-fn* pcypher {}))
  ([pcypher options]
   (fn y
     ([] (y {} {}))
     ([param-data] (y param-data {}))
     ([param-data opts]
      (prepare-cypher pcypher param-data (merge default-cyphervec-options options opts))))))

(defn cyphervec-fn
  "Given an cypher string and optional options, return an
   anonymous function that returns hugcypher format"
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
  "Given a Hugcypher Cypher file, define the <name>-cyphervec functions in the
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
      (compile-exprs ~'pdef)
      (intern-cyphervec-fn ~'pdef ~options))))

(defmacro def-cyphervec-fns-from-string
  ([s] `(def-cyphervec-fns-from-string ~s {}))
  ([s options]
   `(doseq [~'pdef (parsed-defs-from-string ~s)]
      (validate-parsed-def! ~'pdef)
      (compile-exprs ~'pdef)
      (intern-cyphervec-fn ~'pdef ~options))))

(defmacro map-of-cyphervec-fns
  "Given a Hugcypher Cypher file, return a hashmap of database
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
        (validate-parsed-def! ~'pdef)
        (compile-exprs ~'pdef))
      (apply merge
             (map #(cyphervec-fn-map % ~options) ~'pdefs)))))

(defmacro map-of-cyphervec-fns-from-string
  ([s] `(map-of-cyphervec-fns-from-string ~s {}))
  ([s options]
   `(let [~'pdefs (parsed-defs-from-string ~s)]
      (doseq [~'pdef ~'pdefs]
        (validate-parsed-def! ~'pdef)
        (compile-exprs ~'pdef))
      (apply merge
             (map #(cyphervec-fn-map % ~options) ~'pdefs)))))

(defn db-fn*
  "Given parsed cypher and optionally a response, result, audit, debug and options,
  return an anonymous function that can run hugcypher database
  query and supports hugcypher parameter replacement"
  ([parsed-cypher] (db-fn* parsed-cypher :obj :* :default :default {}))
  ([parsed-cypher response] (db-fn* parsed-cypher response :* :default :default {}))
  ([parsed-cypher response result] (db-fn* parsed-cypher response result :default :default {}))
  ([parsed-cypher response result audit] (db-fn* parsed-cypher response result audit :default {}))
  ([parsed-cypher response result audit debug] (db-fn* parsed-cypher response result audit debug {}))
  ([parsed-cypher response result audit debug options]
   (fn y
     ([type conn] (y type conn {} {}))
     ([type conn param-data] (y type conn param-data {}))
     ([type conn param-data opts]
      (let [o (merge default-db-options options opts
                     {:response response :result result
                      :audit audit :debug debug})]
        (as-> parsed-cypher var-x
          (prepare-cypher var-x param-data o)
          ((resolve (debug-fn debug)) var-x param-data)
          (cypher/query type conn var-x)
          ((resolve (audit-fn audit)) conn var-x o
           (:audit options)
           param-data)
          ((resolve (result-fn result)) var-x)
          ((resolve (response-fn response)) var-x)
          ((resolve (output-fn debug)) var-x o)))))))

(defn db-fn
  "Given parsed cypher and optionally a response, result, audit, debug and options,
  return an anonymous function that can run hugcypher database
  query and supports hugcypher parameter replacement"
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
  (let [private-name (:name- header)
        fn-name (symbol (first (or (:name header) private-name)))
        doc (or (first (:doc header)) "")
        response (response-sym header)
        result (result-sym header)
        debug (debug-sym header)
        audit (audit-sym header)
        params-sym {:keys (mapv symbol (:params header))}
        meta (merge (if-let [m (:meta header)]
                      (edn/read-string (string/join " " m)) {})
                    {:doc doc
                     :response response
                     :result result
                     :debug debug
                     :audit audit
                     :file (:file header)
                     :line (:line header)
                     :arglists `([~'type ~'db ~params-sym]
                                 [~'type ~'db ~params-sym]
                                 [~'type ~'db ~params-sym ~'options & ~'command-options])}
                    (when private-name {:private true}))]
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
  "Given a Hugcypher CYPHER file, define the database
   functions in the current namespace.

   Usage:

   (def-db-fns file options?)"
  ([file] `(def-db-fns ~file {}))
  ([file options]
   `(doseq [~'pdef (parsed-defs-from-file ~file)]
      (validate-parsed-def! ~'pdef)
      (compile-exprs ~'pdef)
      (if (snippet-pdef? ~'pdef)
        (intern-cyphervec-fn ~'pdef ~options)
        (intern-db-fn ~'pdef ~options)))))

(defmacro def-db-fns-from-string
  "Given a Hugcypher CYPHER string, define the database
   functions in the current namespace.

   Usage:

   (def-db-fns-from-string s options?)"
  ([s] `(def-db-fns-from-string ~s {}))
  ([s options]
   `(doseq [~'pdef (parsed-defs-from-string ~s)]
      (validate-parsed-def! ~'pdef)
      (compile-exprs ~'pdef)
      (if (snippet-pdef? ~'pdef)
        (intern-cyphervec-fn ~'pdef ~options)
        (intern-db-fn ~'pdef ~options)))))

(defmacro map-of-db-fns
  "Given a Hugcypher CYPHER file, return a hashmap of database
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
        (validate-parsed-def! ~'pdef)
        (compile-exprs ~'pdef))
      (apply merge
             (map
              #(if (snippet-pdef? %)
                 (cyphervec-fn-map % ~options)
                 (db-fn-map % ~options))
              ~'pdefs)))))

(defmacro map-of-db-fns-from-string
  "Given a Hugcypher CYPHER string, return a hashmap of database
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
        (validate-parsed-def! ~'pdef)
        (compile-exprs ~'pdef))
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
  ([type db cypher] (db-run type db cypher {} :obj :* :default :default {}))
  ([type db cypher param-data] (db-run type db cypher param-data :obj :* :default :default {}))
  ([type db cypher param-data response] (db-run type db cypher param-data response :* :default :default {}))
  ([type db cypher param-data response result] (db-run type db cypher param-data response result :default :default {}))
  ([type db cypher param-data response result audit]
   (db-run type db cypher param-data response result audit :default {}))
  ([type db cypher param-data response result audit debug]
   (db-run type db cypher param-data response result audit debug {}))
  ([type db cypher param-data response result audit debug options & command-options]
   (let [f (db-fn cypher response result audit debug options)]
     (f type db param-data command-options))))
