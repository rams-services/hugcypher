(ns hugcypher.parser
  (:require [clojure.string :as string]
            [selmer.parser :as selmer]
            [clojure.tools.logging :as log]
            [clojure.tools.reader.reader-types :as r]
            [clojure.edn :as edn]))

(defn- parse-error
  ([reader msg]
   (parse-error reader msg {}))
  ([reader msg data]
   (if (r/indexing-reader? reader)
     (throw
      (ex-info
       (str msg " line: " (r/get-line-number reader)
            ", column: " (r/get-column-number reader))
       (merge data
              {:line   (r/get-line-number reader)
               :column (r/get-column-number reader)})))
     (throw (ex-info msg (merge data {:error :parse-error}))))))

(defn- sb-append
  [^StringBuilder sb ^Character c]
  (doto sb (.append c)))

(defn- whitespace?
  [^Character c]
  (when c
    (Character/isWhitespace ^Character c)))

(defn- symbol-char?
  [c]
  (boolean (re-matches #"[\pL\pM\pS\d\_\-\.\+\*\?\:\/\$]" (str c))))

(defn- skip-whitespace
  "Read from reader until a non-whitespace char is encountered."
  [reader]
  (loop [c (r/peek-char reader)]
    (when (whitespace? c)
      (do (r/read-char reader)
          (recur (r/peek-char reader))))))

(defn- skip-whitespace-to-next-line
  "Read from reader until a non-whitespace or newline char is encountered."
  [reader]
  (loop [c (r/peek-char reader)]
    (when (and (whitespace? c)
               (not (= \newline c)))
      (do (r/read-char reader)
          (recur (r/peek-char reader))))))

(defn- skip-to-next-line
  "Read from reader until a new line is encountered.
   Reads (eats) the encountered new line."
  [reader]
  (loop [c (r/read-char reader)]
    (when (and c (not (= \newline c)))
      (recur (r/read-char reader)))))

(defn- skip-to-chars
  "Read from reader until the 2 chars are encountered.
   Read (eat) the encountered chars."
  [reader c1 c2]
  (loop [rc (r/read-char reader)
         pc (r/peek-char reader)]
    (if (or (nil? rc) (nil? pc)
            (and (= rc c1) (= pc c2)))
      (do (r/read-char reader) nil) ; read last peek char off, return nil
      (recur (r/read-char reader) (r/peek-char reader)))))

(defn- read-to-char
  "Read and return a string up to the encountered char c.
   Does not read the encountered character."
  [reader c]
  (loop [s (StringBuilder.)
         pc (r/peek-char reader)]
    (if (or (nil? pc) (= c pc))
      (str s)
      (recur (sb-append s (r/read-char reader))
             (r/peek-char reader)))))

(defn- read-to-chars
  "Read and return a string up to the encountered chars.
   Does not read the encountered characters"
  [reader c1 c2]
  (loop [s (StringBuilder.)
         rc (r/read-char reader)
         pc (r/peek-char reader)]
    (if (or (nil? rc) (nil? pc)
            (and (= c1 rc) (= c2 pc)))
      (do (r/unread reader rc) (str s))
      (recur (sb-append s rc)
             (r/read-char reader)
             (r/peek-char reader)))))


(defn- read-param-helper
  [reader character-check]
  (loop [result {}
         string-builder  (StringBuilder.)
         character (r/read-char reader)
         next-character (r/peek-char reader)]
    (cond
      (or (nil? character) (nil? next-character) (not (symbol-char? next-character)))
      (do
        (let [s (str (sb-append string-builder character))]
          (if (> (count s) 0)
            (assoc result :name s)
            (parse-error reader (str "Incomplete parameter :" s)))))

      (= character-check character)
      (recur (assoc result :type (str string-builder))
             (StringBuilder.)
             (r/read-char reader)
             (r/peek-char reader))

      (= \/ character)
      (recur (assoc result :namespace (str string-builder))
             (StringBuilder.)
             (r/read-char reader)
             (r/peek-char reader))

      :else
      (recur result
             (sb-append string-builder character)
             (r/read-char reader)
             (r/peek-char reader)))))


(defn- read-param
  [reader c]
  (read-param-helper reader c))

(defn- read-keyword
  [reader]
  (read-param-helper reader \:))

(defn- split-hash-param
  [param-str & {:keys [reader]}]
  (let [result (string/split param-str #"\$")]
    (if (= (count result) 2)
      result
      (parse-error reader (str "Hash param wrong format: #" param-str)))))

(defn- sing-line-comment-start?
  [c reader]
  (and c (= \- c) (= \- (r/peek-char reader))))

(defn- mult-line-comment-start?
  [c reader]
  (and c (= \/ c) (= \* (r/peek-char reader))))

(defn- quoted-start?
  [c]
  (contains? #{\' \" \`} c))

(defn- unmatched-quoted?
  [c]
  (contains? #{\' \" \`} c))

(defn- param-start?
  [c]
  (= \$ c))

(defn- hash-start?
  [h]
  (= \# h))

(defn- values-vector
  [s]
  (let [reader (r/source-logging-push-back-reader s)]
    (loop [out []
           last-string-so-far ""
           list-opened-counter 0
           current-char (r/read-char reader)
           next-char (r/peek-char reader)]
      (if (nil? current-char)
        (vec (remove #(or (string/blank? %)
                          (empty? %))
                     out))
        (let [counter (case current-char
                        \{ (inc list-opened-counter)
                        \} (dec list-opened-counter)
                        list-opened-counter)
              reached-end-of-string? (and (= counter 0)
                                          (or (nil? next-char)
                                              (whitespace? next-char)))]
          (recur (if reached-end-of-string?
                   (conj out (str (string/trim last-string-so-far) current-char))
                   out)
                 (if reached-end-of-string?
                   "" (str last-string-so-far current-char))
                 counter
                 (r/read-char reader)
                 (r/peek-char reader)))))))

(defn- read-sing-line-expr
  [rdr]
  (let [_    (r/read-char rdr) ; eat ~
        expr (string/trim (read-to-char rdr \newline))]
    [expr :end]))

(defn- read-mult-line-expr
  [rdr]
  (let [_ (r/read-char rdr) ; eat ~
        expr (string/trim (read-to-chars rdr \* \/))
        _    (skip-to-chars rdr \* \/)
        end? (= \~ (last expr))
        expr (if end? (string/trim (string/join "" (butlast expr))) expr)
        sign (if end? :end :cont)]
    (if (string/blank? expr) [sign] [expr sign])))

(defn- read-sing-line-header
  [reader]
  (let [_   (r/read-char reader) ; eat colon (:)
        key (-> reader read-keyword :name keyword)
        line (read-to-char reader \newline)
        values (if (= key :doc)
                 [(string/trim line)]
                 (values-vector line))]
    (skip-to-next-line reader)
    {key values}))

(defn- read-mult-line-header
  [reader]
  (let [_   (r/read-char reader) ; eat colon (:)
        key (-> reader read-keyword :name keyword)
        lines (read-to-chars reader \* \/)
        _     (skip-to-chars reader \* \/)
        values (if (= key :doc)
                 [(string/trim lines)]
                 (values-vector lines))]
    (skip-to-next-line reader)
    {key values}))

(defn- read-sing-line-comment
  [reader c]
  (r/read-char reader) ; eat second dash (-) of comment start
  (skip-whitespace-to-next-line reader)
  (condp = (r/peek-char reader)
    \: (read-sing-line-header reader)
    \~ (read-sing-line-expr reader)
    (skip-to-next-line reader)))

(defn- read-mult-line-comment
  [reader c]
  (r/read-char reader) ; eat second comment char (*)
  (skip-whitespace-to-next-line reader)
  (condp = (r/peek-char reader)
    \: (read-mult-line-header reader)
    \~ (read-mult-line-expr reader)
    (skip-to-chars reader \* \/)))

(defn- read-quoted
  [reader c]
  (let [quot c]
    (loop [s (sb-append (StringBuilder.) c)
           c (r/read-char reader)]
      (condp = c
        nil    (parse-error reader "Cypher String terminated unexpectedly with EOF")
        quot  (let [pc (r/peek-char reader)]
                (if (and pc (= pc quot) (not (= c pc)))
                  (recur (sb-append s c) (r/read-char reader))
                  (str (sb-append s c))))
        (recur (sb-append s c) (r/read-char reader))))))

(defn- read-cypher-param
  [reader c]
  (let [{:keys [name namespace type]} (read-param reader c)]
    {:type (keyword (or type "v"))
     :name (if namespace
             (keyword namespace name)
             (keyword name))}))

(defn read-cypher-map-object
  [reader c]
  (let [{:keys [name namespace type]} (read-param reader c)
        [map-object-name param-name] (split-hash-param name)]
    {:type (keyword (or type "m"))
     :name (if namespace
             (keyword namespace param-name)
             (keyword param-name))
     :map-object (if namespace
                   (keyword namespace map-object-name)
                   (keyword map-object-name))}))

(defn- get-params-for-header [parameters cypher-list header]
  (into
   []
   (clojure.set/union
    (set parameters)
    (selmer/known-variables
     (string/join cypher-list))
    (set (when (:audit header)
           (let [audit (edn/read-string
                        (get-in header [:audit 0]))]
             (concat (when (contains? audit :params)
                       (for [[k v] (:params audit)
                             :when (not-any? #(= k %) [:message :by])]
                         v))
                     (if (not (contains? audit :by))
                         [:by]
                         (if (map? (:by audit))
                           [(get-in audit [:by :param])]
                           [(:by audit)]))
                     (if (not (contains? audit :message))
                       [:message]
                       (when (keyword? (:message audit))
                         [(:message audit)])))))))))

(defn parse
  "Parse hugcypher Cypher string and return
   sequence of statement definitions
   of the form:
   {:header {:name   [\"my-query\"]
             :doc    [\"my doc string\"]
             :command [\":?\"]
             :result [\":1\"]
             :file \"sql/queries.sql\"
             :line 12}
    :cypher [\"MATCH (c:User {id:$id}) RETURN c\"
             {:type :v :name :id}]}

   Throws clojure.lang.ExceptionInfo on error."
  ([cypher-str] (parse cypher-str {}))
  ([cypher-str {:keys [no-header file]}]
   (if (string/blank? cypher-str)
     (throw (ex-info "Cypher is empty" {}))
     (let [cypher-str (string/replace cypher-str "\r\n" "\n")
           reader (r/source-logging-push-back-reader cypher-str)
           new-string-builder #(StringBuilder.)]
       (loop [header {}
              cypher []
              string-builder  (new-string-builder)
              all []
              parameters []]
         (let [character (r/read-char reader)]
           (cond
             ;; end of string, so return all, filtering out empty
             (nil? character)
             (vec
              (remove #(and (empty? (:header %))
                            (or (empty? (:cypher %))
                                (and
                                 (every? string? (:cypher %))
                                 (string/blank? (string/join (:cypher %))))))
                      (let [cypher-list (filterv seq (conj cypher (string/trimr string-builder)))]
                        (conj all
                              {:header (when (not (empty? header))
                                         (assoc
                                          header
                                          :params (get-params-for-header parameters
                                                                             cypher-list
                                                                             header)))
                               :cypher cypher-list}))))

             (or
              (sing-line-comment-start? character reader)
              (mult-line-comment-start? character reader))
             (if-let [x (if (sing-line-comment-start? character reader)
                          (read-sing-line-comment reader character)
                          (read-mult-line-comment reader character))]
               (cond
                 (map? x)
                 (if (or (> (.length ^StringBuilder string-builder) 0) (empty? header))
                   (recur (merge x {:file file :line (max 1 (dec (r/get-line-number reader)))})
                          []
                          (new-string-builder)
                          (let [cypher-list (filterv seq (conj cypher (string/trimr string-builder)))]
                            (conj all
                                  {:header (when (not (empty? header))
                                             (assoc
                                              header
                                              :params (get-params-for-header parameters
                                                                             cypher-list
                                                                             header)))
                                   :cypher (filterv seq (conj cypher (str string-builder)))}))
                          [])
                   (recur (merge header x) cypher string-builder all parameters))
                 ;; hint
                 (string? x)
                 (recur header cypher (sb-append string-builder x) all parameters)
                 :else
                 ;; clj expr was read from comment
                 (recur header (conj cypher (str string-builder) x) (new-string-builder) all parameters))
               (recur header cypher string-builder all parameters))

             (quoted-start? character)
             (recur header cypher (sb-append string-builder
                                            (read-quoted reader character))
                    all parameters)

             (unmatched-quoted? character)
             (parse-error reader (str "Unmatched Cypher quote: " character))

             (hash-start? character)
             (let [param (read-cypher-map-object reader character)]
               (recur header (vec (filter seq
                                   (conj cypher (str string-builder)
                                         param))) (new-string-builder) all (cond (= (:type param) :v)
                            (conj parameters (:name param))
                            (= (:type param) :m)
                            (conj parameters (:name param))
                            :else
                            parameters)))
             
             (param-start? character)
             (let [param (read-cypher-param reader character)]
               (recur header
                      (vec (filter seq
                                   (conj cypher (str string-builder)
                                         param)))
                      (new-string-builder)
                      all
                      (cond (= (:type param) :v)
                            (conj parameters (:name param))
                            (= (:type param) :m)
                            (conj parameters (:name param))
                            :else
                            parameters)))

             :else
             (if (and (not (string/blank? string-builder)) (empty? header) (not no-header))
               (parse-error reader "Encountered Cypher with no hugcypher header")
               (recur header cypher (sb-append string-builder character)
                      all parameters)))))))))
