## Handling Corrupt in CSV data files

updated on: 17-2-2025

Corrupt values, and null values in source data files are a usual thing. They are unavoidable, and we have to deal with them.
Spark provides us with methods with which, we can handle the corrupted and null values.

### How to handle csv files with null values and corrupted values
To ensure that spark correctly identifies the data type of the csv values, we have to define the schema explicitly.

Consider the csv file `corruptData.csv`
```csv
Roll,Name,Final Marks,Float Marks,Double Marks
1,Ajay,300,55.5,
2,Bharghav,350,63.2,88.5
3,Chaitra,320,60.1,75.8
4,Kamal,360,75.0,82.3
5,Sohaib,gchbnv,70.8,90.6
```

```scala
val schema = StructType(Seq(
        StructField("Roll", IntegerType, true),
        StructField("Name", StringType, true),
        StructField("Final Marks", IntegerType, true),
        StructField("Float Marks", FloatType, true),
        StructField("Double Marks", DoubleType, true)
      ))
```
Let us now try to read the csv file and see what will be the output
```scala
val df=spark.read.schema(schema).option("header","true")
      .csv("csvFiles/corruptData.csv")
df.show()
```
When a csv file with corrupted values and null values is read in spark, both the null values and corrupted values are displayed as `NULL`.

**Output**
```text
+----+--------+-----------+-----------+------------+
|Roll|    Name|Final Marks|Float Marks|Double Marks|
+----+--------+-----------+-----------+------------+
|   1|    Ajay|        300|       55.5|        NULL|
|   2|Bharghav|        350|       63.2|        88.5|
|   3| Chaitra|        320|       60.1|        75.8|
|   4|   Kamal|        360|       75.0|        82.3|
|   5|  Sohaib|       NULL|       70.8|        90.6|
+----+--------+-----------+-----------+------------+
```

### How to print the list of corrupted values as a separate column?
We can use the method `option("mode", "PERMISSIVE")` which creates another column of corrupted values.
We also need to add another column to store corrupted values, since we are defining the schema explicitly. This means that we have to redefine the schema of the table.
```scala
val schema1 = StructType(Seq(
  StructField("Roll", IntegerType, true),
  StructField("Name", StringType, true),
  StructField("Final Marks", IntegerType, true),
  StructField("Float Marks", FloatType, true),
  StructField("Double Marks", DoubleType, true),
  StructField("Bad records",StringType,true)
))

val permissiveDf=spark.read.option("header","true")
  .schema(schema1)
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "Bad records")
  .csv("csvFiles/corruptData.csv")

permissiveDf.show()
```
**Output**
```text
+----+--------+-----------+-----------+------------+-------------------------+
|Roll|Name    |Final Marks|Float Marks|Double Marks|Bad records              |
+----+--------+-----------+-----------+------------+-------------------------+
|1   |Ajay    |300        |55.5       |NULL        |NULL                     |
|2   |Bharghav|350        |63.2       |88.5        |NULL                     |
|3   |Chaitra |320        |60.1       |75.8        |NULL                     |
|4   |Kamal   |360        |75.0       |82.3        |NULL                     |
|5   |Sohaib  |NULL       |70.8       |90.6        |5,Sohaib,gchbnv,70.8,90.6|
+----+--------+-----------+-----------+------------+-------------------------+
```

### How to drop the rows with corrupted values of a csv file?
To handle this situation, we use the `option("mode", "DROPMALFORMED")` which omits the rows with corrupted values
```scala
val corruptDf=spark.read.option("header","true")
      .schema(schema)
      .option("mode", "DROPMALFORMED")
      .csv("csvFiles/corruptData.csv")
    
corruptDf.show()
```
```text
+----+--------+-----------+-----------+------------+
|Roll|    Name|Final Marks|Float Marks|Double Marks|
+----+--------+-----------+-----------+------------+
|   1|    Ajay|        300|       55.5|        NULL|
|   2|Bharghav|        350|       63.2|        88.5|
|   3| Chaitra|        320|       60.1|        75.8|
|   4|   Kamal|        360|       75.0|        82.3|
+----+--------+-----------+-----------+------------+
```
Since the last record had a corrupted value, only that row has been deleted. The first row's last column value is a Null value, so that is not deleted.

### How to replace the null values and corrupt values?
To replace the null values, we use the dataframe's null value methods. To know how to implement those methods, [Refer this article](@/docs/spark/null-values.md)

To save the updates done on the csv file, [Refer this article on how to save csv file](@/docs/spark/handling-csv-format-files.md)

### Summary
In this article we have seen
- How to handle csv files which has null values
- How to create a column that stores the details of the rows with null values

### Related articles
- [Handling Null Values](@/docs/spark/null-values.md)
- [Handling CSV files](@/docs/spark/handling-csv-format-files.md)

### Refernces
- [How to handle corrput csv files?](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)
