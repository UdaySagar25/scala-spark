## CSV file schema handling

created on: 14-2-2025

### How to define the schema of a csv file?
For any given csv file, spark automatically understands the schema with the help of the flag `infershcema`.

Let us take the below csv file as an example
```csv
Roll,Name,Marks
1,Ajay,55
2,Bharghav,63
3,Chaitra,60
4,Kamal,75
5,Sohaib,70
```
To define the shchema, we use the flag `inferschema`
```scala
val df=spark.read.option("inferschema","true").option("header","true")
      .csv("csvFiles/students.csv")

df.show()
df.printSchema()
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
### How to define the custom schema of the csv file?
We can use the method `schema()` which applies the defined schema on the csv file.
```scala
val defineSchema = StructType(Seq(
      StructField("Roll", IntegerType, true),
      StructField("Name", StringType, true),
      StructField(" Marks", DoubleType, true)
    ))

val df1=spark.read.schema(defineSchema).option("header","true")
  .csv("csvFiles/students.csv")

df1.show()
df1.printSchema()
```
**Output**
```text
+----+--------+------+
|Roll|    Name| Marks|
+----+--------+------+
|   1|    Ajay|  55.0|
|   2|Bharghav|  63.0|
|   3| Chaitra|  60.0|
|   4|   Kamal|  75.0|
|   5|  Sohaib|  70.0|
+----+--------+------+

root
 |-- Roll: integer (nullable = true)
 |-- Name: string (nullable = true)
 |--  Marks: double (nullable = true)
```
### How does `enforceSchema()` work?
`enforceSchema()` is a method in spark csv file handling which allows the user to enforce the defined schema onto the csv files.
When `enforceSchema()` is set to false, all the datatypes are read as **StringType**
```scala
val df2=spark.read.option("enforceSchema", "false")
      .option("header","true")
      .csv("csvFiles/students.csv")

df2.show()
df2.printSchema()
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
 |-- Roll: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- Marks: string (nullable = true)
```

If `enforceSchema()` is set to true, spark will enforce the defined schema onto the csv file.
```scala
val enfSchema = StructType(Seq(
      StructField("Roll", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Marks", FloatType, true)
    ))

    val df2=spark.read.option("enforceSchema", "true")
      .schema(enfSchema)
      .option("header","true")
      .csv("csvFiles/students.csv")

df2.show()
df2.printSchema()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay| 55.0|
|   2|Bharghav| 63.0|
|   3| Chaitra| 60.0|
|   4|   Kamal| 75.0|
|   5|  Sohaib| 70.0|
+----+--------+-----+

root
 |-- Roll: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- Marks: float (nullable = true)
```

### Summary
In this article, 
- We have looked at different ways to define the schema of the csv file.
- We have seen how `inferSchema` flag automatically understands the schema of the csv file.
- We have seen how to define the custom schema?
- We have seen how `enforceSchema` flag works in defining the schema.

### Related Articles
- [Handle csv files](handleCsv.md)
- [Working with corrupt csv files](csvCorrput.md)
- [csv file opeartions](csvOps1.md)

### References
- [Provide Schema while reading csv files as a dataframe in Spark](https://stackoverflow.com/questions/39926411/provide-schema-while-reading-csv-file-as-a-dataframe-in-scala-spark)
- [Handling csv files in Spark](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)
- [Spark custom shcema for a csv file](https://stackoverflow.com/questions/46246392/spark-custom-schema-for-csv-file?rq=1)
