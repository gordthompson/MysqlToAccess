# MysqlToAccess

A simple Java console application to dump the tables from a MySQL database 
into a newly-created Access database file.

**Usage:**

The path of the output file is provided as the first (and only)
command-line parameter, e.g., 

    java -jar MysqlToAccess.jar /home/gord/accessfile.accdb

MySQL connection properties and other runtime options are controlled by a 
`MysqlToAccess.properties` file that resides in the same folder as 
the `main` class (or runnable JAR) file.

**Dependencies:**

This application uses *MySQL Connector/J* to read the MySQL database,
*Jackcess* to write the Access database file, and *Apache Commons IO* for
filespec juggling. See the Maven dependencies for more information.
