# MIPMapReduced
### Available functionality

#### - Translate instance and export to database

~~~
[path to jar]/MIPMapReduced.jar [path to mapping task]/mapping_task_simple.xml [path to database config file]/db.properties -db [path to export database config file]/exportdb.properties
~~~

#### - Translate instance and export to csv file

~~~
[path to jar]/MIPMapReduced.jar [path to mapping task]/mapping_task_simple.xml [path to database config file]/db.properties -csv [path to export folder]
~~~

#### - Unpivot csv file
~~~
-unpivot [path to csv]/inputCsv.csv [path to database config file]/db.properties nameOfNewColumn {-i(gnore) or -u(npivot} [path to the text file with the columns]/columns.txt
~~~

__Comments__

1. If you choose `-i` the specified columns will be ignored from the unpivot procedure. On the other hand, if you choose `-u` the columns will be used for the unpivot procedure

2. In the `columns.txt` the columns must specified line by line

#### - Change csv delimeter
~~~
-csv_delimeter [path to csv]/inputCsv.csv {";" or ":" or tab} {single or double}
~~~ 

__Comments__

1. You can choose among three options about the input csv current delimeter
    * ";" : If the current csv is separated with semi-colon
    * ":" : If the current csv is separated with colon
    * tab : If the current csv is separated with tabs

2. You can choose among three options about the input csv current quote style
    * single : If the current csv includes single quotes
    * double : If the current csv includes double quotes

#### - Generate id using columns from source to target table
~~~
-generate_id [path to properties file]/file.properties
~~~

__Properties file commands__

1. Specify the source input
    ~~~
    commandSource={csv | db}
    ~~~

2. Specify source input path
    ~~~
    sourceInputPath= {[path to csv file]/file.csv | [path to database properties file],[name of the table to extract data]
    ~~~

3. Specify the target input
    ~~~
    commandTarget={csv | db}
    ~~~

4. Specify target input path
    ~~~
    targetInputPath= {[path to csv file]/file.csv | [path to database properties file],[name of the table to extract data]}
    ~~~

5. Names of the target columns
    ~~~
    targetColumns=[col11,col22,col33,generated_id]
    ~~~

6. Specify a transformation function to the source columns

   If no transformation function is selected the actual data from the source input are used.

   If any string value lies inside quotes, every tuple in the specific column have this value.
    ~~~
    functionPerColumn=[split(col1,_,0),"constant value",col3]
    ~~~

7. Output csv file
    ~~~
    outputFile=[path to csv file]/file.csv
    ~~~ 