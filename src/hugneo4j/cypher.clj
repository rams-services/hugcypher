(ns hugneo4j.cypher
  (:require [clojurewerkz.neocons.bolt :as neobolt]
            [selmer.parser :as parser]
            [clojure.tools.logging :as log]))

(defn- is-driver-obj? [v]
  (or (= (type v) org.neo4j.driver.internal.InternalRelationship)
      (= (type v) org.neo4j.driver.internal.InternalNode)))

(defn- is-random-access-list? [v]
  (= (type v) java.util.Collections$UnmodifiableRandomAccessList))

(defn- get-datetime-helper []
  (let [fmt (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ssZ")]                       
    (.format fmt
             (.getTime
              (doto (java.util.Calendar/getInstance)
                (.setTime (java.util.Date.))
                (.add java.util.Calendar/HOUR_OF_DAY 1))))))

(let [default-audit-params (atom {:node nil
                                  :id-attribute nil
                                  :default-id nil
                                  :relationship "AuditTrail"
                                  :connecting-node "AuditNode"
                                  :made-relation "AuditMadeOf"})]

  (defn set-audit-params
    [& {:keys [node-label node-id-attribute
               default-node-id audit-relationship
               audit-node audit-made-relationship]}]
    (reset! default-node-id {:node (if node-label
                                     node-label
                                     (:node @default-audit-params))
                             :id-attribute (if node-id-attribute
                                             node-id-attribute
                                             (:id-attribute @default-audit-params))
                             :relationship (if audit-relationship
                                             audit-relationship
                                             (:relationship @default-audit-params))
                             :connecting-node (if audit-node
                                                audit-node
                                                (:connecting-node @default-audit-params))
                             :made-relation (if audit-made-relationship
                                              audit-made-relationship
                                              (:made-relation @default-audit-params))
                             :default-id (if default-node-id
                                           default-node-id
                                           (:default-id @default-audit-params))}))
  
  (defn connect
    ([database-url]
     (connect database-url nil nil))
    ([database-url username]
     (connect database-url username nil))
    ([database-url username password]
     (neobolt/connect database-url username password)))

  (defn query
    [conn query-info-list]
    (with-open [session (neobolt/create-session conn)]
      (let [query-list (first query-info-list)]
        [(neobolt/query session
                        (first query-list)
                        (apply merge {} (second query-list)))
         (second query-info-list)])))

  (defn without-audit
    [conn query-responses-list options audit-params param-data]
    query-responses-list)
  
  (defn audit
    [conn query-responses-list options audit-params data-params]
    (let [query-responses (first query-responses-list)]
      (with-open [session (neobolt/create-session conn)]
        (let [audit-nodes (if (:node audit-params)
                            [(:node audit-params)]
                            (if (vector? (:nodes audit-params))
                              (:nodes audit-params)
                              [(:nodes audit-params)]))]
          (doseq [query-response (if (map? query-responses)
                                   [query-responses]
                                   query-responses)]
            (neobolt/query session
                           (str "MATCH (`audit-by`:" (if (and (map? (:by audit-params))
                                                              (get-in audit-params [:by :node]))
                                                       (get-in audit-params [:by :node])
                                                       (:node @default-audit-params))
                                ") WHERE `audit-by`.`" (if (and (map? (:by audit-params))
                                                                (get-in audit-params [:by :attribute]))
                                                         (get-in audit-params [:by :attribute])
                                                         (:id-attribute @default-audit-params))
                                "`=$`" (if (not (contains? audit-params :by))
                                         "by"
                                         (if (map? (:by audit-params))
                                           (name (get-in audit-params [:by :param]))
                                           (name (:by audit-params)))) "` "
                                (clojure.string/join
                                 " " (for [[k v] query-response
                                           :when (and (is-driver-obj? v)
                                                      (some #(= k (if (symbol? %)
                                                                    (name %) %))
                                                            audit-nodes))]
                                       (str "MATCH (`" k "`:"
                                            (clojure.string/join
                                             ":" (.labels v))
                                            ") WHERE ID(`" k "`)=$`"
                                            k "-id`")))
                                "CREATE (`audit-by`)-[:" (:relationship @default-audit-params)
                                " $`audit-props`]->"
                                (if (> (count audit-nodes) 1)
                                  (str "(`audit-with`:" (:connecting-node @default-audit-params) ") "
                                       (clojure.string/join
                                        " " (for [ky audit-nodes]
                                              (str "CREATE (`audit-with`)-[:"
                                                   (:made-relation @default-audit-params)
                                                   "]->(`" ky "`)"))))
                                  (str "(`" (first audit-nodes) "`)")))
                           (let [param-name (if (map? (:by audit-params))
                                              (name (get-in audit-params [:by :param]))
                                              (name (:by audit-params)))]
                             (merge {param-name (get data-params (keyword param-name))
                                     "audit-props" {"message" (if (not (contains? audit-params :message))
                                                                (get data-params :message)
                                                                (if (keyword? (:message audit-params))
                                                                  (get data-params
                                                                       (keyword (:message audit-params)))
                                                                  (:message audit-params)))
                                                    "created-on" (get-datetime-helper)}}
                                    (into {}
                                          (for [[k v] query-response
                                                :when (is-driver-obj? v)]
                                            [(str k "-id") (.id v)])))))))))
    query-responses-list))


(defn- listify-helper
  [item]
  (let [list-keys (loop [its (keys item)
                         out {}]
                    (if (empty? its)
                      out
                      (let [it (first its)
                            val (get item it)]
                        (recur (rest its)
                               (if (is-driver-obj? val)
                                 (merge out
                                        (into {}
                                              (for [k (.keys val)]
                                                [k (conj (get out k) it)])))
                                 (assoc out it
                                        (conj (get out it) nil)))))))]
    (loop [its (keys item)
           out {}]
      (if (empty? its)
        out
        (let [it (first its)
              val (get item it)]
          (recur (rest its)
                 (cond
                   (is-random-access-list? val)
                   (assoc out (keyword it)
                          (map listify-helper val))
                   (is-driver-obj? val)
                   (let [value-map (.asMap val)]
                     (apply merge out
                            (for [[k v] value-map]
                              (if (= (count (get list-keys k)) 1)
                                (assoc out (keyword k) v)
                                (assoc out (keyword (str it "." k)) v)))))
                   true
                   (assoc out (keyword it) val))))))))

(defn- objectify-helper
  [item]
  (into {}
        (for [[k v] item]
          [(keyword k) (cond
                         (is-random-access-list? v)
                         (map (fn [temp]
                                (listify-helper (if (is-driver-obj? temp)
                                                  (.asMap temp)
                                                  temp)))
                              v)
                         (is-driver-obj? v)
                         (into {}
                               (for [[sub-k sub-v] (.asMap v)]
                                 [(keyword sub-k) sub-v]))
                         true v)])))
(defn to-bool
  [query-responses-list]
  [(not (empty? (first query-responses-list)))
   (second query-responses-list)])

(defn to-list
  [query-responses-list]
  (let [query-responses (first query-responses-list)]
    [(if (map? query-responses)
        (listify-helper query-responses)
        (map listify-helper query-responses))
     (second query-responses-list)]))

(defn to-obj
  [query-responses-list]
  (let [query-responses (first query-responses-list)]
    [(if (map? query-responses)
        (objectify-helper query-responses)
        (map objectify-helper query-responses))
     (second query-responses-list)]))

(defn first-result
  [query-responses-list]
  [(first (first query-responses-list))
   (second query-responses-list)])

(defn all-results
  [query-responses-list]
  query-responses-list)

(defn no-monitor
  [query-responses-list options]
  (first query-responses-list))

(defn save-monitor
  [query-responses-list options]
  (when (second query-responses-list)
    (spit "performance.db.log" (pr-str
                                {:fn (:fn-name options)
                                 :msecs (double
                                         (/ (- (System/nanoTime)
                                               (second query-responses-list))
                                            1e6))})
          :append true))
  (first query-responses-list))


(defn no-debug-needed
  [query-responses param-data]
  [[(parser/render (first query-responses) param-data)
    (second query-responses)]
   nil])

(defn debug
  [query-responses param-data]
  (let [query-str (parser/render (first query-responses) param-data)]
    (log/debug "[CYPHER] " query-str
               "\n|||\n[PARAMS] " (apply merge (second query-responses)))
    [[query-str (second query-responses)] nil]))

(defn monitor
  [query-responses param-data]
  [[(parser/render (first query-responses) param-data)
    (second query-responses)]
   (System/nanoTime)])

(defn debug-and-monitor
  [query-responses param-data]
  (let [debug-results (debug query-responses param-data)]
    [(first debug-results) (System/nanoTime)]))

