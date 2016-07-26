(defproject db-cp "0.1.0-SNAPSHOT"
  :description "copy db tables from db to db"
  :url "https://github.com/mangosmoothie/db-cp"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [com.oracle.database/ojdbc6 "11.2.0.4"]
                 [org.clojure/java.jdbc "0.6.0-alpha2"]
                 [com.mchange/c3p0 "0.9.5.2"]]
  :main db-cp.core)
