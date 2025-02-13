## Dataframe

### What is a Dataframe ?
A dataframe is a collection of data with a set of named columns. It is similar to SQL table where the data is stored in rows and columns. They are built on top of RDDs and are usually used to process large amounts of data.

First let's create a spark session, then we will start with creating an empty dataframe. To create a Spark session refer the [Spark Session Page](https://namastespark.com/docs/spark/spark-session/)

### How to create an empty dataframe ?
```scala
val df = spark.emptyDataFrame
df.show()
```
**Output**
```text
++
||
++
++
```

### How to define a schema for a dataframe ?
```scala
// We will be creating a dataframe of marks obtained by students in their final exam.
val schema = StructType(Seq(
      StructField("Roll", IntegerType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Marks", IntegerType, nullable = true)
    ))
// Show Empty DataFrame
emptyDF.show()
```
**Output**
```text
+----+----+-----+
|Roll|Name|Marks|
+----+----+-----+
+----+----+-----+
```
### How to create a DataFrame from a CSV File
Here we will look into creating a dataframe from an already existing source file. We'll be using CSV file as source file.
```scala
// Read CSV File into DataFrame
val df: DataFrame = spark.read
  .option("header", "true")  // Use first row as column names
  .option("inferSchema", "true")
  .option("encoding", "UTF-8")
  .csv("marks.csv")

df.show()
```
**Output**
```text
+----+--------+-----+
|Roll|   Name |Marks|
+----+--------+-----+
|   1|   Ajay |   55|
|   2|Bharghav|   63|
|   3|Chaitra |   60|
|   4|  Kamal |   75|
|   5|  Sohaib|   70|
+----+--------+-----+
```

### How to read the schema of the dataframe?
To read the schema of the dataframe, use the `.printSchema()` method.
```scala
// prints the shcema of the dataframe
df.printSchema()
```
**Output**
```text
root
 |-- Roll: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- Marks: integer (nullable = true)
```

There is another method to read the schema of the dataframe. `df.schema` method can be used to display the dataframe's structure.
```scala
//alternate way to print the schema of the created DataFrame
println(emptyDF.schema)
```
**Output**
```text
StructType(StructField(Roll,IntegerType,true),StructField(Name,StringType,true),StructField(Marks,IntegerType,true))
```

### How to convert DataFrames to RDD?
You can convert a DataFrame to RDD using the `.rdd` method.
```scala
import spark.implicits._

// Create a DataFrame with column names
val df: DataFrame = Seq(
  (1, "Ajay", 55),
  (2, "Bharghav", 63),
  (3, "Chaitra", 60),
  (4, "Kamal", 75),
  (5, "Sohaib", 70)
).toDF("Roll", "Name", "Marks")

// Convert DataFrame to RDD[Row]
val rddFromDF: RDD[Row] = df.rdd

// Convert RDD[Row] to RDD with specific types
val typedRDD: RDD[(Int, String, Int)] = rddFromDF.map(row =>
  (row.getAs[Int]("Roll"), row.getAs[String]("Name"), row.getAs[Int]("Marks"))
)
// Print the RDD
typedRDD.collect().foreach(println)
```
**Output**
```text
(1,Ajay,55)
(2,Bharghav,63)
(3,Chaitra,60)
(4,Kamal,75)
(5,Sohaib,70)
```

### How to convert Dataframes to Datasets?
```scala
import spark.implicits._
// Define a case class for Dataset
case class Marks(Roll: Int, Name: String, Marks: Int)

// Create a DataFrame
val df: DataFrame = Seq(
  (1, "Ajay", 55),
  (2, "Bharghav", 63),
  (3, "Chaitra", 60),
  (4, "Kamal", 75),
  (5, "Sohaib", 70)
).toDF("Roll", "Name", "Marks")

// Convert DataFrame to Dataset[Marks]
val ds: Dataset[Marks] = df.as[Marks]

// Show the Dataset
ds.show()
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

### Summary
We have covered basic concepts of Spark DataFrames, starting from its definition, schema creation, conversion of Dataframes to other data structures.
We now know the salient features of DataFrames, including
- `df.schema()` and `df.printSchema()` returns the schema(skeleton) of the dataframe.
- Reading data from CSV files, using the method `read.csv`
- Converting Dataframes to RDD using `.rdd` and to datasets using `as.[T]`
- How DataFrames are converted to RDDs and Datasets.

### References
- [Create DataFrames ](https://learn.microsoft.com/en-us/azure/databricks/getting-started/dataframes#create-dataframe)
- [DataFrames](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Create RDDs](https://stackoverflow.com/questions/32531224/how-to-convert-dataframe-to-rdd-in-scala)
