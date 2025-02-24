## JSON file null and corrupt values parsing

In any given time, there is a high possibility that we will be dealing with JSON files which might have null values or some files might have corrupted values.
When we try to parse these files in Spark, the session is interrupted and the data is not parsed correctly, leading to improper data interpretation.

In this article, we will be exploring different methods to handle null values and corrupted values using Spark configurations for handling JSON files.

### There are few corrupted values in the JSON file. How can I parse JSON files which have corrupted values?
Consider the JSON file `corruptedValues.json`
```json
[
  {
    "Roll": 1,
    "Name": "Ajay",
    "Marks": "fifty five"
  },
  {
    "Roll": 2,
    "Name": "Bharghav",
    "Marks": 63
  },
...
```
This file has few corrupted values in the **Marks** field. When parsed by Spark, it is read as StringType, whereas the expected data type should be IntegerType.
Generally when the JSON file is parsed, parsing marks column will throw error. But spark, provides us with an option to permit the function to read the data as is.
It is called as `PERMISSIVE`. To read the faulty values, we set `PERMISSIVE` to true.
```scala
val df = spark.read
  .option("multiLine", "true")
  .option("mode", "PERMISSIVE")
  .json("jsonFiles/corruptedValues.json")
df.show()
```
**Output**
```text 
+----------+--------+----+
|     Marks|    Name|Roll|
+----------+--------+----+
|fifty five|    Ajay|   1|
|        63|Bharghav|   2|
|     sixty| Chaitra|   3|
|        75|   Kamal|   4|
|        70|  Sohaib|   5|
+----------+--------+----+
```
Surprisingly, the default value to read the file is also set to `PERMISSIVE`. The only drawback is that the data type is changes while parsing.

### In the JSON, there is a field with all the null values. How to ignore that column and read the JSON file?
Consider the JSON file `allNullValues.json`, where the values of **city** are `null`
```json
[
  {
    "Roll": 1,
    "Name": "Ajay",
    "Marks": 55,
    "city": null
  },
  {
    "Roll": 2,
    "Name": "Bharghav",
    "Marks": 63,
    "city": null
  },
...
```
The field `city` has null values for all the records.
```scala
val allNullValues=spark.read.option("multiLine","true")
     .json("jsonFiles/allNullValues.json")

   allNullValues.printSchema()
   allNullValues.show()
```
If we execute the above spark code, null values are also printed, occupying more memory which is of no use.
**Output**
```text
root
 |-- Marks: long (nullable = true)
 |-- Name: string (nullable = true)
 |-- Roll: long (nullable = true)
 |-- city: string (nullable = true)

+-----+--------+----+----+
|Marks|    Name|Roll|city|
+-----+--------+----+----+
|   55|    Ajay|   1|null|
|   63|Bharghav|   2|null|
|   60| Chaitra|   3|null|
|   75|   Kamal|   4|null|
|   70|  Sohaib|   5|null|
+-----+--------+----+----+
```

Spark has an inbuilt configuration `dropFieldIfAllNull` which will ignore the column and read the rest of the JSON file. This configuration comes handy when We do not want unnecessary columns to be printed while working with large datasets.
```scala
 val ignoreNullValues=spark.read.option("multiLine","true")
     .option("dropFieldIfAllNull","true")
     .json("jsonFiles/allNullValues.json")

ignoreNullValues.printSchema()
ignoreNullValues.show()
```

When the configurations are set to true, the column `city` is dropped.

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

### Summary
In this article, we have seen:
- How spark handles JSON files if there is improper data found in the file.
- How we can handle null values that are present in the JSON file.

### Related Articles
- [Spark JSON Datetime parsing](@/docs/spark/spark-json-datetime-parsing.md)
- [Spark JSON file value parsing](@/docs/spark/spark-json-file-value-parsing.md)

### References 
- [Spark JSON documentation](https://spark.apache.org/docs/3.5.4/sql-data-sources-json.html)
- [Representing Null in JSON](https://stackoverflow.com/questions/21120999/representing-null-in-json)
