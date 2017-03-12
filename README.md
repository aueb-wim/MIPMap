# MipMapReduced
### Available functionality

- Translate instance and export to database

~~~
[path to jar]/MIPMapReduced.jar [path to mapping task]/mapping_task_simple.xml [path to database config file]/db.properties -db [path to export database config file]/exportdb.properties
~~~

- Translate instance and export to csv file

~~~
[path to jar]/MIPMapReduced.jar [path to mapping task]/mapping_task_simple.xml [path to database config file]/db.properties -csv [path to export folder]
~~~

- Unpivot csv file
~~~
-unpivot [path to csv]/inputCsv.csv [path to database config file]/db.properties nameOfNewColumn {-i(gnore) or -u(npibot} [path to the text file with the columns]/columns.txt
~~~

__Comments__

1. If you choose -i the specified columns will be ignored from the unpivot procedure. On the other hand, if you choose -u the columns will be used for the unpivot procedure
2. In the column.txt the columns must specified line by line 