(ns com.kaicode.dsync.db
  #?(:cljs (:require-macros [cljs.core.async.macros :refer [go-loop go]]))
  
  (:require #?(:clj [datomic.api :as d])
            #?(:cljs [datascript.core :as d])

            #?(:clj [cprop.core :refer [load-config]])
            #?(:clj [cprop.source :as source])
            
            #?(:clj [clojure.tools.logging :as log])

            #?(:cljs [cljs-uuid-utils.core :as uuid])
            #?(:cljs [com.kaicode.mercury :as m])
            
            #?(:cljs [com.kaicode.wocket.client :as ws])

            [com.kaicode.tily :as tily]
            [mount.core :as mount]
            [clojure.core.async :as a :refer [<! >! chan]]
            [clojure.walk :as w]))

(declare conn)

#?(:clj
   (do
     (mount/defstate env :start (load-config
                                 :merge
                                 [(mount/args)
                                  (source/from-system-props)
                                  (source/from-env)
                                  (source/from-resource "schema.edn")
                                  (source/from-resource "test.data.edn")]))
     
     (defn disconnect [conn]
       (let [url (env :db-url)]
         (log/info "disconnecting from " url)
         (.release conn)
         (d/delete-database url)))

     (defn tempid []
       (d/tempid :db.part/user))

     (mount/defstate conn
       :start  (let [url (env :db-url)
                     db-created? (d/create-database url)
                     conn (d/connect url)]
                 (log/info "url" url)
                 (log/info "db-created?" db-created?)
                 (when-not (env :production)
                   (let [schema (env :datomic-schema)
                         test-data (env :test-data)]
                     (if schema
                       (do
                         (log/debug "schema" schema)
                         @(d/transact conn schema))
                       (log/fatal "no schema defined"))

                     (if test-data
                       (do
                         (log/debug "test-data" test-data)
                         @(d/transact conn test-data))
                       (log/debug "no test-data defined"))))
                 conn)
       :stop  (disconnect conn))))

(defn get-db []
  #?(:clj (d/db conn))
  #?(:cljs @conn))

#?(:cljs
   (do
     (def when-ds-ready (m/whenever :datascript/ready))

     (m/on :schema/available (fn [[_ schema]]
                               (def conn (d/create-conn schema))
                               (m/broadcast [:datascript/ready conn])))

     (ws/send! [:export-schema true])
     
     (defonce query-params->channel (atom {}))
     (defonce query-params-queue (atom []))
     (defn q-channel
       "like q except returns a channel containing the result of the (q params)"
       [& params]
       (let [channel (or (@query-params->channel params)
                         (let [new-channel (chan 2)]
                           (tily/set-atom! query-params->channel [params] new-channel)
                           new-channel))
             q-params (tily/insert-at (vec params) 1 (get-db))]
         (go (>! channel (apply d/q q-params)))
         channel))
     
     (go-loop []
       (doseq [query-params (distinct @query-params-queue)
               :let [q-params (tily/insert-at (vec query-params) 1 (get-db))
                     channel (@query-params->channel query-params)]]
         
         (>! channel (apply d/q q-params)))
       (reset! query-params-queue [])
       (<! (a/timeout 2000))
       (recur))))



(defn squuid []
  #?(:clj (str (d/squuid)))
  #?(:cljs (uuid/make-random-uuid)))

(defn system-id []
  #?(:clj (squuid))
  #?(:cljs (-> (squuid) uuid/uuid-string)))

(defn entity [id]
  (d/entity (get-db) id))

(defn q
  "wrapper around d/q so that you don't have to pass in the current database"
  [& params]
  (let [params (into [] params)
        params (tily/insert-at (vec params) 1 (get-db))]
    (apply d/q params)))

(defn conjugate [kw]
  (let [nspace (namespace kw)
        n (name kw)]
    (keyword (str nspace "/_" n))))

(defn extract-kw [col]
  (->> col
       (w/postwalk (fn [e]
                     (cond
                       (or (keyword? e)
                           (seq? e)
                           (sequential? e)) e
                       (map? e) (concat (keys e) (extract-kw (vals e)))
                       :else nil)))
       flatten
       distinct
       (remove nil?)))

(defn extract-entity-kw [col]
  (->> col extract-kw (remove #(let [n (namespace %)]
                                 (or (= n "system")
                                     (= n "db")
                                     (= :find %)
                                     (= :in %)
                                     (= :where %))))))

(defn transact [tx]
  #?(:clj (d/transact conn tx))
  #?(:cljs (let [tx-report (d/transact! conn tx)
                 tx-kws (extract-entity-kw tx)
                 run-query? (fn [query-kws]
                              (let [r (some (fn [tx-kw]
                                              (or (tily/is-contained? tx-kw :in query-kws)
                                                  (tily/is-contained? (conjugate tx-kw) :in query-kws)
                                                  (= tx-kw :db.fn/retractEntity)))
                                            tx-kws)]
                                ;;(println r ": " tx-kws query-kws)
                                r))]
             (doseq [[query-params channel] @query-params->channel
                     :let [query (first query-params)
                           query-kws (extract-entity-kw query)]
                     :when (run-query? query-kws)]
               (swap! query-params-queue conj query-params))
             tx-report)))

(defn touch [e]
  (prn "touch" (type e))
  (when e
    (d/touch e)))

(defn entity? [e]
  #?(:cljs (=  datascript.impl.entity/Entity (type e)))
  #?(:clj (= datomic.query.EntityMap (type e))))

(defn touch-all [e]
  "touch the entity e and all referenced entities recursively"
  (when (entity? e)
    (d/touch e)
    (doseq [[a v] e]
      (cond
        (set? v) (doseq [e2 (a e)]
                   (touch-all e2))
        (entity? v) (touch-all (a e)))))
  e)

(defn- create-pull [pattern]
  (concat '(pull ?e) [pattern]))

(defn- create-find [pull-pattern where-pattern]
  (vec (concat [:find]
               [[(create-pull pull-pattern)
                 '...]]
               [:where] where-pattern)))


(defn create-datomic-find-in-namespace [entity-namespace pull-pattern & where]
  (let [my-ns [(concat '(= ?ns) [entity-namespace])]
        where-pattern (vec (concat  '[[?e ?aid ?v] [?aid :db/ident ?a] [(namespace ?a) ?ns]] [my-ns] where))
        where-patttern (into where-pattern where)]
    (create-find pull-pattern
                 where-pattern)))

(defn create-datascript-find-in-namespace [entity-namespace pull-pattern & where]
  (let [my-ns [(concat '(= ?ns) [entity-namespace])]
        where-pattern (vec (concat '[[?e ?a] [(?namespace ?a) ?ns]] [my-ns]))
        where-pattern (into where-pattern where)
        find (vec (concat [:find]
                          [[(create-pull pull-pattern)
                            '...]]
                          '[:in $ ?namespace]
                          [:where] where-pattern))]
    find))

(defn map->entity [m]
  (when-not (empty? m)
    (let [id (:system/id m)]
      (some->> [:system/id id]
               (d/entity (get-db))
               touch-all))))

(defn entity->map [e]
  (cond
    (or
     (entity? e) (map? e)) (reduce-kv (fn [m k v] (assoc m k (entity->map v))) {} (into {} e))
    (coll? e) (map entity->map e)
    :else e))

(comment
  (q '[:find ?e :in $ ?namespace :where [?e ?a] [(?namespace ?a) ?ns] [(= ?ns "item")]] namespace)
  (q '[:find ?e :where [?e :item/name _]])

  (d/q '[:find ?e :where [?e :item/name "Chocolate"]] @conn)
  (q '[:find ?e :where [?e :item/name "Chocolate"]])

  (q '[:find ?e :in $ ?namespace :where [?e ?a] [(?namespace ?a) ?ns] [(= ?ns "item")]] namespace)
  
  (q '[:find ?e :in $ ?namespace :where [?e ?a] [(?namespace ?a) ?ns] [(= ?ns "type")]] namespace)

  (q '[:find [(pull ?e [*]) ...] :in $ ?namespace :where [?e ?a] [(?namespace ?a) ?ns] [(= ?ns "type")]] namespace)
  (q '[:find [(pull ?e [*]) ...] :in $ ?namespace :where [?e ?a] [(?namespace ?a) ?ns] [(= ?ns "type")]] namespace)
  (q '[:find [(pull ?e [*]) ...] :where [?e ?a] [(?namespace ?a) ?ns] [(= ?ns "type")]])

  (db/create-datascript-find-in-namespace "type" '[* {:type/items [*]}] '[foo 1] '[foo 2])
  )
