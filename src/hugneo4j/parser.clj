(ns hugneo4j.parser
  (:require [clojure.string :as string]
            [clojure.tools.reader.reader-types :as r]))

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
  [reader]
  (read-param-helper reader \$))

(defn- read-keyword
  [reader]
  (read-param-helper reader \:))

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
    (skip-to-next-line reader)))

(defn- read-mult-line-comment
  [reader c]
  (r/read-char reader) ; eat second comment char (*)
  (skip-whitespace-to-next-line reader)
  (condp = (r/peek-char reader)
    \: (read-mult-line-header reader)
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

(defn- read-neo4j-param
  [reader c]
  (let [{:keys [name namespace type]} (read-param reader)]
    {:type (keyword (or type "v"))
     :name (if namespace
             (keyword namespace name)
             (keyword name))}))

(defn parse
  "Parse hugneo4j Cypher string and return
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
  ([neo4j-str] (parse neo4j-str {}))
  ([neo4j-str {:keys [no-header file]}]
   (if (string/blank? neo4j-str)
     (throw (ex-info "Neo4j is empty" {}))
     (let [neo4j-str (string/replace neo4j-str "\r\n" "\n")
           reader (r/source-logging-push-back-reader neo4j-str)
           new-string-builder #(StringBuilder.)]
       (loop [header {}
              neo4j []
              string-builder  (new-string-builder)
              all []]
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
                      (conj all
                            {:header header
                             :cypher (filterv seq (conj neo4j (string/trimr string-builder)))})))

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
                          (conj all
                                {:header header
                                 :cypher (filterv seq (conj neo4j (str string-builder)))}))
                   (recur (merge header x) neo4j string-builder all))
                 ;; hint
                 (string? x)
                 (recur header neo4j (sb-append string-builder x) all)
                 :else
                 ;; clj expr was read from comment
                 (recur header (conj neo4j (str string-builder) x) (new-string-builder) all))
               (recur header neo4j string-builder all))

             (quoted-start? character)
             (recur header neo4j (sb-append string-builder
                                            (read-quoted reader character))
                    all)

             (unmatched-quoted? character)
             (parse-error reader (str "Unmatched Neo4j quote: " character))

             (param-start? character)
             (recur header
                    (vec (filter seq
                                 (conj neo4j (str string-builder)
                                       (read-neo4j-param reader character))))
                    (new-string-builder)
                    all)

             :else
             (if (and (not (string/blank? string-builder)) (empty? header) (not no-header))
               (parse-error reader "Encountered Neo4j with no hug4neo4j header")
               (recur header neo4j (sb-append string-builder character) all)))))))))
