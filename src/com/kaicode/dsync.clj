(ns com.kaicode.dsync
  (:require [datomic.api :as d]
            [com.kaicode.dsync.db :as db]
            [com.kaicode.wocket.server :as ws :refer [process-msg]]
            [com.kaicode.mercury :as m]
            [clojure.walk :as w]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]))

(defn entity [id]
  (d/entity (db/get-db) id))

(defn pull [pattern eid]
  (d/pull (db/get-db) pattern eid))

(defn entity? [e]
  (= (type e) datomic.query.EntityMap))

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

(defonce query->ws-client-channels (atom {}))

(defn- subscribe [client-websocket-channel _ query]
  (swap! query->ws-client-channels update-in [query] (fn [ws-set]
                                                       (if (empty? ws-set)
                                                         #{client-websocket-channel} 
                                                         (conj ws-set client-websocket-channel)))))

(defn- unsubscribe [disconnected-client-websocket-channel]
  (doseq [[query socket-channels] @query->ws-client-channels
          :let [connected-ws-client-channels (filter (fn [sc] (not= sc disconnected-client-websocket-channel))
                                                     socket-channels)]]
    (swap! query->ws-client-channels update-in [query]
           (constantly connected-ws-client-channels))))

(m/on :client-websocket-channel-closed (fn [[_ client-websocket-channel]]
                                         (unsubscribe client-websocket-channel)))

(defn assoc-db-id [m]
  (w/postwalk (fn [m]
                (if (map? m)
                  (if (contains? m :db/id)
                    m
                    (update-in m [:db/id] (constantly (db/tempid))))
                  m))
              m))

(let [coerce-double (fn [x]
                      (cond
                        (number? x) (double x)
                        (string? x) (try
                                      (if (empty? x)
                                        0.0
                                        (Double/parseDouble x))
                                      (catch Exception e
                                        ::s/invalid))
                        :else ::s/invalid))
      c (s/conformer coerce-double)]
  (s/def :item/price c)
  (s/def :product/amount c)
  (s/def :gps/lat c)
  (s/def :gps/lng c)
  )


(defn number->double [m]
  (w/postwalk (fn [m]
                (if (map? m)
                  (cond
                    (contains? m :product/amount) (assoc m :product/amount (s/conform :product/amount (:product/amount m)))
                    (contains? m :item/price) (assoc m :item/price (s/conform :item/price (:item/price m)))
                    (contains? m :gps/lat) (assoc m :gps/lat (s/conform :gps/lat (:gps/lat m)))
                    (contains? m :gps/lng) (assoc m :gps/lng (s/conform :gps/lng (:gps/lng m)))
                    :else m)
                  m))
              m))

(defn dissoc-db-id [m]
  (w/postwalk #(cond
                 (and (map? %) (contains? % :db/id))  (dissoc % :db/id)
                 (and (sequential? %)) (mapv dissoc-db-id %)
                 :else %)
              m))

(defmethod process-msg :remote-q [[client-websocket-channel [_ query & params]]]
  (subscribe client-websocket-channel :to query)
  (let [q-params (into [query] params)
        q-r (->> q-params (apply db/q) dissoc-db-id)]
    (ws/send! client-websocket-channel [:remote-q-result [q-params q-r]])))

(defmethod process-msg :remote-transact [[client-websocket-channel [_ tx-from-client]]]
  (try (let [tx (->> tx-from-client macroexpand eval
                     number->double)
             tx-with-db-id (assoc-db-id tx)
             client-websocket-channels (->> @ws/client-websocket-channels distinct
                                            (remove #(or (= % client-websocket-channel)
                                                         (nil? %))))]
         (log/debug "tx-with-db-id" tx-with-db-id)
         (db/transact tx-with-db-id)
         (doseq [a-ws client-websocket-channels]
           (ws/send! a-ws [:transact tx])))
       (catch Exception ex (let [msg [:remote-transact-error [tx-from-client (. ex toString)]]]
                             (log/error msg)
                             (ws/send! client-websocket-channel msg)))))

(defn entity+
  [db eid]
  (cond
    (integer? eid) (d/entity db eid)
    (:db/id eid) (d/entity db (:db/id eid))))

(defn touch+
  "By default, touch returns a map that can't be assoc'd. Fix it"
  [ent]
  ;; (into {}) makes the map assoc'able, but lacks a :db/id, which is annoying for later lookups.
  (into (select-keys ent [:db/id]) (d/touch ent)))

(defn remove-nils [m]
  "remove nil values from map m"
  (into {} (filter second m)))

(defn transform-to-datascript-dbValueType
  "datascript components must have :db.type/ref. all other entities do not require :db/valueType. having one will throw an exception.
  remove :db/valueType if ent is not a component."
  [ent]
  (cond
    (= (:db/isComponent ent) true) (update-in ent [:db/valueType] (constantly :db.type/ref))
    (= (:db/valueType ent) :db.type/ref) ent
    :else (dissoc ent :db/valueType)))

(defn export-schema []
  (let [db (db/get-db)
        schema (->> db
                    (d/q '[:find [?e ...]
                           :where
                           [?e :db/ident ?ident]])
                    (map (partial entity+ db))
                    (map touch+))
        schema (->> schema
                    (filter (fn [ent]
                              (let [kns (namespace (:db/ident ent))
                                    kns (if-not (nil? kns)
                                          (subs kns 0 (min 2 (count kns))))]
                                (and (not (nil? kns)) (not= "db" kns)
                                     (not (get #{"fressian"} (namespace (:db/ident ent))))))))
                    (map transform-to-datascript-dbValueType)
                    (sort-by :db/ident))
        schema (->> schema (reduce (fn [run entry]
                                     "datascript schema has a different edn format than datomic. datascript schema are of the form {:enttity {...}}. for example:
                                      {:address/primary  {:db/fulltext false
                                                          :db/cardinality :db.cardinality/one
                                                          :db/id 151,
                                                          :db/ident :license/date-issued}}"
                                     (assoc run (:db/ident entry) entry)) {})
                    remove-nils)]
    schema))

(defmethod process-msg :export-schema [[websocket-channel [kw msg]]]
  (let [schema (export-schema)]
    (ws/send! websocket-channel [:schema schema])))
