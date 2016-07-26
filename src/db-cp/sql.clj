(ns db-cp.sql
  (:require [clojure.java.jdbc :as db]
            [clojure.string :as s])
  (:import [java.sql DriverManager ResultSet]))

(defn make-insert-sql [table-name col-names]
  (str "INSERT INTO " table-name " (" (s/join ", " col-names) ") "
       "VALUES (" (s/join "," (repeat (count col-names) "?")) ")"))

(defn mk-conn-str [db-spec]
  (str "jdbc:" (:subprotocol db-spec) ":" (:subname db-spec)))

(defn make-conn [db-spec]
  (DriverManager/getConnection (mk-conn-str db-spec)
                               (:user db-spec) (:password db-spec)))

(defn keys->col-names [keys]
  (map #(.substring (str %) 1) keys))

(defn get-primary-keys
  "returns seq of keywords of table's primary key column(s)
  ->table-name expected format = SCHEMA.TABLE"
  [table-name db-spec]
  (let [[schema tbl] (s/split table-name #"\.")]
    (map #(keyword (s/lower-case (:column_name %)))
         (db/with-db-metadata [meta db-spec]
           (db/metadata-query (.getPrimaryKeys meta nil schema tbl))))))
