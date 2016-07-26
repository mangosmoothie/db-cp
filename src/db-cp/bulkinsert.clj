(ns db-cp.bulkinsert
  (:require [db-cp.sql :refer [make-insert-sql keys->col-names make-conn]])
  (:import oracle.jdbc.OraclePreparedStatement))

(defn vals->vec [keyvec dmap]
  (map #(% dmap) keyvec))

(defn do-bulk-insert
  "bulk insert using oracle's driver for batched writes. very fast."
  [db-spec table-name datamap]
  (let [conn (make-conn db-spec)
        col-keys (keys (first datamap))
        col-names (keys->col-names col-keys)
        insert-sql (make-insert-sql table-name col-names)
        stmt (.prepareStatement conn insert-sql)]
    (.setExecuteBatch (cast OraclePreparedStatement stmt) 10000)
    (println "doing it for:" (count datamap) "rows")
    (loop [d datamap]
      (when (seq d)
        (loop [c (map #(% (first d)) col-keys)
               i 1]
          (when (seq c)
            (.setObject stmt i (first c))
            (recur (rest c) (inc i))))
        (.executeUpdate stmt)
        (recur (rest d))))
    (.close stmt)
    (.close conn)))
