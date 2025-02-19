## Handling JSON files in Spark

created on: 18-02-2025

As we know, JSON is one of the format used to store and exchange data in readable way.
Spark is capable of reading and writing JSON data, also making it more flexible to manipulating the data.

In this article, we will look into how JSON files are read in Spark, and what all operations can be done on the files.

To begin with, we will first read the JSON file using `.json(file_path.json)` method.

### How to read data from JSON files in spark?
Assume we have a simple JSON file `singleLine.json` with few data records in it.
```json
{"name": "Alice", "age": 30, "city": "New York"}
{"name": "Bob", "age": 25, "city": "Los Angeles"}
{"name": "Charlie", "age": 35, "city": "Chicago"}
```
To read this file, we implement `.json(file_path.json)`

```scala
val singleLine=spark.read.json("jsonFiles/singleLine.json")

singleLine.show()
singleLine.printSchema()
```
**Output**
```text
 +---+---------+------+
|Age|     City|  Name|
+---+---------+------+
| 30|New Delhi| Anish|
| 25|  Lucknow|Bhavya|
| 35|  Chennai|Charan|
+---+---------+------+

root
 |-- Age: long (nullable = true)
 |-- City: string (nullable = true)
 |-- Name: string (nullable = true)
```
Very well....

We have understood how to read simple JSON files in Spark. Let's advance bit and see how to read the JSON files where each record is a multiline value.
Consider the JSON file `students.json`
```json
[
  {
    "Roll": 1,
    "Name": "Ajay",
    "Marks": 55
  },
  {
    "Roll": 2,
    "Name": "Bharghav",
    "Marks": 63
  },
  {
    "Roll": 3,
    "Name": "Chaitra",
    "Marks": 60
  },
  {
    "Roll": 4,
    "Name": "Kamal",
    "Marks": 75
  },
  {
    "Roll": 5,
    "Name": "Sohaib",
    "Marks": 70
  }
]
```
### How to read a JSON file with multiline data?
Let us try the previous version of reading the JSON file and see if we are able to read it or not.
```scala
val multiline=spark.read.json("jsonFiles/students.json")
    
multiline.show()
multiline.printSchema()
```
**Output**
```text
Exception in thread "main" org.apache.spark.sql.AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
referenced columns only include the internal corrupt record column

  root
 |-- _corrupt_record: string (nullable = true)
```
Reading the multiline JSON file raises an error stating that the data is corrupted. Even the schema shows that there exists corrupt data.Now how to deal with it?

We can use the method `.option("multiLine","true")` to read the multiline data.

```scala
val jsonDf=spark.read.option("multiLine","true").json("jsonFiles/students.json")

jsonDf.printSchema()
jsonDf.show()
```
**Output**
```text
root
 |-- Marks: long (nullable = true)
 |-- Name: string (nullable = true)
 |-- Roll: long (nullable = true)

+-----+--------+----+
|Marks|    Name|Roll|
+-----+--------+----+
|   55|    Ajay|   1|
|   63|Bharghav|   2|
|   60| Chaitra|   3|
|   75|   Kamal|   4|
|   70|  Sohaib|   5|
+-----+--------+----+
```
Now the problem is resolved! The data from the JSON file is being read correctly and the schema is being correctly inferred.

It is advised to use `option("multiLine","true")` as it makes easier to read complex and long JSON files.

You might ask, "There are many cases where the data is stored in nested JSON format, how to deal with those files?"
Let's answer that question as well.

### How to read data from a nested JSON file?
Similar to reading the multiline JSON file, here as well we use `option("multiLine","true")` to read nested JSON files.
```scala
val stdDetails=spark.read.option("multiLine","true").json("jsonFiles/studentDetails.json")

    stdDetails.printSchema()
    stdDetails.show(truncate=false)
```
**Output**
```text
 root
 |-- Contact: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- mail: string (nullable = true)
 |    |    |-- mobile: string (nullable = true)
 |-- Marks: long (nullable = true)
 |-- Name: string (nullable = true)
 |-- Roll: long (nullable = true)

+-------------------------------+-----+--------+----+
|Contact                        |Marks|Name    |Roll|
+-------------------------------+-----+--------+----+
|[{ajay@mail.com, 8973 113}]    |55   |Ajay    |1   |
|[{bharghav@mail.com, 9876 205}]|63   |Bharghav|2   |
|[{chaitra@mail.com, 7789 656}] |60   |Chaitra |3   |
|[{kamal@mail.com, 8867 325}]   |75   |Kamal   |4   |
|[{sohaib@mail.com, 9546 365}]  |70   |Sohaib  |5   |
+-------------------------------+-----+--------+----+
```

### Summary
In this article, we have seen:
- How to read a  simple JSON file in spark.
- What happens if a complex JSON file is read similar to simple JSON file.
- How to read a multiline and nested JSON file.

### References
- [Spark JSON documentation](https://spark.apache.org/docs/3.5.4/sql-data-sources-json.html)
- [Reading JSON files](https://docs.databricks.com/en/query/formats/json.html)
- [Reading Json file using Apache Spark](https://stackoverflow.com/questions/40212464/reading-json-file-using-apache-spark)
