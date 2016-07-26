# db-cp

Clojure library designed to copy the contents of one database table into another - usually in a separate database. Option to perform bulk-copy or merge-into.

The two databases should pretty much mirror each other. An example use is to copy data from a development environment to a local environment.

Only setup and tested on Oracle Databse 12c with ojdbc7 driver.

## Setup

Download the [ojdbc7.jar](http://www.oracle.com/technetwork/database/features/jdbc/jdbc-drivers-12c-download-1958347.html) from Oracle and place it on your classpath.

## Usage

Bulk copy using the super-fast OracleBulkInsert. Destination table should be empty in most cases.

```lein run bulk-copy <tablename>```

Merge two database tables together. Will ignore rows in the source table where the primary key already exists the destination table.

```lein run merge-to <tablename>```


## License

Copyright © 2016 Nathan Lloyd

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
