(ns db-cp.core
  (:require [clojure.java.jdbc :as db]
            [clojure.pprint :refer [pprint]]
            [db-cp.bulkinsert :refer [do-bulk-insert]]
            [db-cp.sql :refer [get-primary-keys]])
  (:import [java.sql PreparedStatement]
           com.mchange.v2.c3p0.ComboPooledDataSource
           oracle.jdbc.OraclePreparedStatement))

(def conns {:copy-to
            {:user "USER"
             :password "PASSWORD"
             :subprotocol "oracle"
             :subname "thin:@localhost:1521/**SERVICENAME**"
             :classname "oracle.jdbc.driver.OracleDriver"}
            :copy-from
            {:user "USER"
             :password "PASSWORD"
             :subprotocol "oracle"
             :subname "thin:@**HOSTNAME**:1521/**SERVICENAME**"
             :classname "oracle.jdbc.driver.OracleDriver"}})

(defn pool
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               (.setMaxIdleTimeExcessConnections (* 30 60))
               (.setMaxIdleTime (* 3 60 60)))]
    {:datasource cpds}))

(def pooled-db (delay (pool (:local conns))))

(defn db-connection [] @pooled-db)

(defn map->vector [keyvec inmap]
  (vec (map #(% inmap) keyvec)))

(defn make-pk
  "makes a map containing a pk - according to the pk-keywords"
  [pk-keywords record]
  (reduce
   (fn [acc n]
     (assoc acc n (n record)))
   {}
   pk-keywords))

(defn read-all
  "Read all rows from table and return map of results"
  [table-name db-spec]
  (db/query db-spec (vector (str "SELECT * FROM " table-name))))

(defn read-all-pkset
  "Read all rows from table, put pks into set"
  [table-name db-spec]
  (let [pk-columns (get-primary-keys table-name db-spec)
        data (read-all table-name db-spec)]
    (into #{}
          (map #(make-pk pk-columns %) data))))

(defn insert-all
  "Insert all data (vectors) into table - batch update"
  [table-name datamaps]
  (db/insert-multi! (db-connection) table-name datamaps))

(defn merge-to
  "merges the data from source to target"
  [table-name db-spec-from db-spec-to]
  (let [pkset (read-all-pkset table-name db-spec-to)
        pk-kwords (keys (first pkset))]
    (do-bulk-insert db-spec-to
                    table-name
                    (filter #(not (contains? pkset (make-pk pk-kwords %)))
                            (read-all table-name db-spec-from)))))

(defn bulk-copy [tablename]
  (println "starting bulk copy")
  (do-bulk-insert (:local conns) tablename
                  (read-all tablename (:dev conns))))

(defn -main
  [job table-in]
  (let [tablename (clojure.string/upper-case table-in)]
    (case job
      "bulk-copy" (bulk-copy tablename)
      "merge-to" (merge-to tablename (:dev conns) (:local conns)))))
