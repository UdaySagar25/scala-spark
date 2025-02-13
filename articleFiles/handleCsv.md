## Handling CSV format files

created on: 13-2-2025

One of the format used for storing data is CSV format. It is possible to handle CSV data in spark using Dataframe-API.

Let us now look into handling CSV data using spark.

### How to load csv files in spark framework?
To load the csv files in spark framework, we use `spark.read`, followed by `.option()` and `.csv(filepath)`.
```csv
Roll,Name,Marks
1,Ajay,55
2,Bharghav,63
3,Chaitra,60
4,Kamal,75
5,Sohaib,70
```
```scala
//reading a csv file
val readCsv= spark.read.option("header", "true")
  .option("inferSchema", "true")
  .option("encoding", "UTF-8")
  .csv("students.csv")

readCsv.show()
readCsv.printSchema()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   55|
|   2|Bharghav|   63|
|   3| Chaitra|   60|
|   4|   Kamal|   75|
|   5|  Sohaib|   70|
+----+--------+-----+

root
 |-- Roll: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- Marks: integer (nullable = true)
```

### How to handle csv files which has different delimiter?
Assume that we have a csv file with a delimiter(;). To read the data with semicolon(;) as delimiter, we specify the delimiter in the `option("delimiter",";")` method.
```csv
Roll;Name;Marks
1;Ajay;55
2;Bharghav;63
3;Chaitra;60
4;Kamal;75
5;Sohaib;70
```
The above csv file can be loaded as given below
```scala
val delimiterCsv= spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .option("delimiter",";")
      .csv("students1.csv")
delimiterCsv.show()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   55|
|   2|Bharghav|   63|
|   3| Chaitra|   60|
|   4|   Kamal|   75|
|   5|  Sohaib|   70|
+----+--------+-----+
```
Similarly, if there are other types of delimiters, say `|` or `" "`, we define them in the `option()` method. 

### How to select columns from a csv data?
We use `select(col1, col2,...colN)` to select the desired columns from the csv data
```scala
val selectCols=readCsv.select("Name","Marks")
selectCols.show()
```
**Output**
```text
+--------+-----+
|    Name|Marks|
+--------+-----+
|    Ajay|   55|
|Bharghav|   63|
| Chaitra|   60|
|   Kamal|   75|
|  Sohaib|   70|
+--------+-----+
```
### How to filter rows of a csv data with specific values ?
We use the `filter()` method to filter out the rows which have specific values
```scala
val selectVals= readCsv.filter("Marks >= 63")
selectVals.show()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   2|Bharghav|   63|
|   4|   Kamal|   75|
|   5|  Sohaib|   70|
+----+--------+-----+
```

### How to rename the column of a scv file?
We can use `withColumnRenamed()` method to rename the column of the csv file
```scala
val colName=readCsv.withColumnRenamed("Marks", "Math Marks")
colName.show()
```
**Output**
```text
+----+--------+----------+
|Roll|    Name|Math Marks|
+----+--------+----------+
|   1|    Ajay|        55|
|   2|Bharghav|        63|
|   3| Chaitra|        60|
|   4|   Kamal|        75|
|   5|  Sohaib|        70|
+----+--------+----------+
```
### How to save the changes made to the csv files?
To save the changes, we use `write()` method, followed by `.csv(folderpath)`.
```scala
colName.write.format("csv")
      .mode("overwrite")
      .option("header","true")
      .csv("output")
```
This saves the output file in the `output` directory.

### Summary
Throughout this article, we have seen how spark allows accessing data from the csv source files.
We have also looked into
- How to the csv files
- How to read csv files when the delimiter is different(";" , ",", "|")
- How to select the select required columns
- How to filter the rows based on conditions
- How to format the headers of the csv file
- And finally, how to save the changes made to the csv file

### References
- [Reading csv files](https://docs.databricks.com/en/query/formats/csv.html)
- [Loading a csv file](https://stackoverflow.com/questions/29704333/spark-load-csv-file-as-dataframe)
- [Handling csv file operations](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)


