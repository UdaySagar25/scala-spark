## JSON file operations

Assume we have a JSON file containing details of students and their marks in the recent series of exams. We want to read that in a specific data type.
To do this,we have to define the schema explicitly and then put it on top of the JSON file.

We have `studentMarks.json` file with us.
```json
[
  {
    "Roll": 1,
    "Name": "Ajay",
    "Final Marks": 300,
    "Float Marks": 55.5,
    "Double Marks": 92.75
  },
  {
    "Roll": 2,
    "Name": "Bharghav",
    "Final Marks": 350,
    "Float Marks": 63.2,
    "Double Marks": 88.5
  },
...
```

### How to read a JSON file with a custom schema?
When spark automatically infers the schema of the JSON file, there is a possibility that the data type might be wrongly interpreted. 
For example:
```scala
val stdMarks=spark.read.option("multiLine","true")
      .option("inferSchema","true")
      .json("jsonFiles/studentMarks.json")

stdMarks.printSchema()
```
**Output**
```text
  root
 |-- Double Marks: double (nullable = true)
 |-- Final Marks: long (nullable = true)
 |-- Float Marks: double (nullable = true)
 |-- Name: string (nullable = true)
 |-- Roll: long (nullable = true)
```
We can see that the column's data type has been interpreted wrong. `Roll` is interpreted as Long type, whereas it is supposed to be of Short type.
To correct this, we will define our own schema and call it using `schema(schema_name)` method

```scala
val ownSchema = StructType(Seq(
      StructField("Roll", ShortType, true),
      StructField("Name", StringType, true),
      StructField("Final Marks", IntegerType, true),
      StructField("Float Marks", FloatType,true),
      StructField("Double Marks", DoubleType,true)
    ))

    val schMarks=spark.read.option("multiLine","true").schema(ownSchema)
      .json("jsonFiles/studentMarks.json")

    schMarks.printSchema()
```
**Output**
```text
root
 |-- Roll: short (nullable = true)
 |-- Name: string (nullable = true)
 |-- Final Marks: integer (nullable = true)
 |-- Float Marks: float (nullable = true)
 |-- Double Marks: double (nullable = true)
```
We can see that the data types of all the columns have been corrected.


### How can I define custom schema for a nested JSON file?
Similar to defining custom schema for regular JSON file, for nested JSON file, we define the schema in a nested way. 
During our previous discussion, we worked on a nested JSON file, we will be defining a custom schema for that.  
Refer this article [Handling JSON files in Spark](@/docs/spark/handling-json-files-in-spark.md)

```scala
val sch = StructType(Seq(
      StructField("Roll", ShortType, true),
      StructField("Name", StringType, true),
      StructField("Final Marks", IntegerType, true),
      StructField("Contact", StructType(Seq(
        StructField("Mail", StringType, true),
        StructField("Mobile", StringType, true)
      )), true)
    ))
val nestedSchema=spark.read.option("multiLine","true")
  .schema(sch).json("jsonFiles/studentDetails.json")

nestedSchema.printSchema()
```
**Output**
```text
root
 |-- Roll: short (nullable = true)
 |-- Name: string (nullable = true)
 |-- Final Marks: integer (nullable = true)
 |-- Contact: struct (nullable = true)
 |    |-- Mail: string (nullable = true)
 |    |-- Mobile: string (nullable = true)
```

### Summary
In this article, we have seen that :
- When spark reads JSON files using schema inference, it may lead to data type misinterpretation
- How custom schema definition can help us tackle the above problem
- How to define schema for a nested JSON file; define a structured schema using `StructField` inside `StructType`.

### References
- [Spark from_json with dynamic schema](https://stackoverflow.com/questions/49088401/spark-from-json-with-dynamic-schema)
- [Spark JSON documentation](https://spark.apache.org/docs/3.5.4/sql-data-sources-json.html)
- [How to read json with schema in spark dataframes/spark sql?](https://stackoverflow.com/questions/39355149/how-to-read-json-with-schema-in-spark-dataframes-spark-sql)



