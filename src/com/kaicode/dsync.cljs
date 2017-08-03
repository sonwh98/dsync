(ns com.kaicode.dsync
  (:require-macros [cljs.core.async.macros :refer [go-loop go]])
  (:require [com.kaicode.wocket.client :as ws :refer [process-msg]]
            [cljs.core.async :refer [<! >! put! chan]]
            [com.kaicode.dsync.db :as db]
            [com.kaicode.mercury :as m]
            [com.kaicode.tily :as tily]
            [konserve.core :as k]
            [konserve.indexeddb :refer [new-indexeddb-store]]))

(def query->channel (atom {}))

(declare datascript-schema-store)
(go (defonce datascript-schema-store (<! (new-indexeddb-store "datascript-schema-store"))))

(defmethod process-msg :remote-transact-error [[tx error]]
  (m/broadcast [tx error]))

(defmethod process-msg :remote-q-result [[_ [query+params remote-result]]]
  (let [channel (@query->channel query+params)]
    (put! channel remote-result)))

(defn remote-transact
  "send transaction to remote datomic"
  [tx]
  (ws/send! [:remote-transact tx]))

(defn transact
  "transact to local datascript and remote datomic"
  [tx]
  (db/transact tx)
  (remote-transact tx))

(defmethod process-msg :schema [[_ schema-from-datomic]]
  (go
    (defonce datascript-schema-store (<! (new-indexeddb-store "datascript-schema-store")))
    (let [stored-schema (<! (k/get-in datascript-schema-store [:schema]))]
      (prn "stored-schema?" (not (empty? stored-schema)))
      (when (not=  stored-schema schema-from-datomic)
        (prn "storing schema")
        (k/assoc-in datascript-schema-store [:schema] schema-from-datomic))
      (m/broadcast [:schema/available schema-from-datomic]))))

(defmethod process-msg :transact [[_ tx]]
  (db/transact tx))

(defn remote-q-channel [query & params]
  (let [query+params+as-vec (into [query] params)
        channel (or (@query->channel query+params+as-vec) (chan 10))
        cmd (into [:remote-q query] params)]
    (swap! query->channel update-in [query+params+as-vec] (constantly channel))
    (ws/send! cmd)
    channel))

(defn datomic->datascript [query & params]
  (go (let [in-result-channel (apply remote-q-channel (into [query] params))
            result (<! in-result-channel)
            tx-report (db/transact result)]
        tx-report)))

(defn delete-rows-fn [rows]
  (let [tx (vec (for [r rows]
                  [:db.fn/retractEntity [:system/id (:system/id r)]]))]
    (transact tx)))

(defn data-loop [{:keys [datomic-query-n-params datascript-query-n-params on-data-available]}]
  (db/when-ds-ready #(let [q-result-channel (apply db/q-channel (or datascript-query-n-params datomic-query-n-params ))]
                       (when-not (empty? datomic-query-n-params)
                         (let [ds-tx-report-channel (apply datomic->datascript datomic-query-n-params)]
                           (go (let [tx-report (<! ds-tx-report-channel)]))))
                       
                       (go-loop []
                         (let [result (<! q-result-channel)]
                           (on-data-available result))
                         (recur)))))
