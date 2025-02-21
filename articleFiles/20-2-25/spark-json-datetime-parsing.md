## Spark JSON Datetime parsing

In today's world, date and time play a vital role in every domain. From tracking financial activities to tracking exam time of the students, everywhere date and time are used. 
However, there are always obstacles while working with date and time data, because the format might vary from place to place and this will impact future analysis.

To overcome this, Spark provides us with options that help us read date and time of different formats. In this article, we shall explore those options.

Consider we have a JSON file `studentSubmitData.json`
```json
{
    "Roll": 1,
    "Name": "Ajay",
    "Age": 14,
    "DOB": "01/01/2010",
    "Submit Time": "2025/02/17 12:30:45",
    "Final Marks": 92.7
  },
  {
    "Roll": 2,
    "Name": "Bharghav",
    "Age": 15,
    "DOB": "04/06/2009",
    "Submit Time": "2025/02/17 12:35:30",
    "Final Marks": 88.5
  },
...
```
### How to parse date values in a JSON file?

JSON does not have a built-in `Date` or `TimeStamp` format. It only stores values in `String` type `Numeric` type or other equivalent types. 
Thus, the column `DOB` is treated as `String` 

```scala
val submitData=spark.read.option("multiLine","true")
  .option("inferSchema","true")
  .json("jsonFiles/studentSubmitData.json")

submitData.printSchema()
```

**Output**
```text
root
 |-- Age: long (nullable = true)
 |-- DOB: string (nullable = true)
 |-- Final Marks: double (nullable = true)
 |-- Name: string (nullable = true)
 |-- Roll: long (nullable = true)
 |-- Submit Time: string (nullable = true)
 
+---+----------+-----------+--------+----+-------------------+
|Age|       DOB|Final Marks|    Name|Roll|        Submit Time|
+---+----------+-----------+--------+----+-------------------+
| 14|01/01/2010|       92.7|    Ajay|   1|2025/02/17 12:30:45|
| 15|04/06/2009|       88.5|Bharghav|   2|2025/02/17 12:35:30|
| 13|12/12/2010|       75.8| Chaitra|   3|2025/02/17 12:45:10|
| 14|25/08/2010|       82.3|   Kamal|   4|2025/02/17 12:40:05|
| 13|14/04/2009|       90.6|  Sohaib|   5|2025/02/17 12:55:20|
+---+----------+-----------+--------+----+-------------------+
```
We can see that the columns **DOB** and **Submit Time** are considered as strings and prints the values as it is.
If we want to the DOB column in `Date` format, then we have to explicitly convert it. To do this, we import `to_date` function from `sql.functions._`
```scala
import org.apache.spark.sql.functions._

val submitData = spark.read.option("multiLine", "true")
      .option("inferSchema", "true")
      .json("jsonFiles/studentSubmitData.json")
      .withColumn("DOB", to_date($"DOB", "dd/MM/yyyy"))

submitData.printSchema()
submitData.show()
```
**Output**
```text
root
 |-- Age: long (nullable = true)
 |-- DOB: date (nullable = true)
 |-- Final Marks: double (nullable = true)
 |-- Name: string (nullable = true)
 |-- Roll: long (nullable = true)
 |-- Submit Time: string (nullable = true)
 
+---+----------+-----------+--------+----+-------------------+
|Age|       DOB|Final Marks|    Name|Roll|        Submit Time|
+---+----------+-----------+--------+----+-------------------+
| 14|2010-01-01|       92.7|    Ajay|   1|2025/02/17 12:30:45|
| 15|2009-06-04|       88.5|Bharghav|   2|2025/02/17 12:35:30|
| 13|2010-12-12|       75.8| Chaitra|   3|2025/02/17 12:45:10|
| 14|2010-08-25|       82.3|   Kamal|   4|2025/02/17 12:40:05|
| 13|2009-04-14|       90.6|  Sohaib|   5|2025/02/17 12:55:20|
+---+----------+-----------+--------+----+-------------------+
```

### How to parse timestamp values in a JSON file?
Similar to handling date format, we will use `to_timestamp` function, inside `withColumn()` and define the format of timestamp format. `to_timestamp` should also be imported from `sql.functions._`
```scala
val submitTime = spark.read.option("multiLine", "true")
      .option("inferSchema", "true")
      .json("jsonFiles/studentSubmitData.json")
      .withColumn("DOB", to_date($"DOB", "dd/MM/yyyy"))
      .withColumn("Submit Time", to_timestamp($"Submit Time","yyyy/MM/dd HH:mm:ss"))


    submitTime.printSchema()
    submitTime.show()
```
Now that we have formatted the timestamp and date, explicitly, both the variables will be treated as datetime type
**Output**
```text
root
 |-- Age: long (nullable = true)
 |-- DOB: date (nullable = true)
 |-- Final Marks: double (nullable = true)
 |-- Name: string (nullable = true)
 |-- Roll: long (nullable = true)
 |-- Submit Time: timestamp (nullable = true)
 
+---+----------+-----------+--------+----+-------------------+
|Age|       DOB|Final Marks|    Name|Roll|        Submit Time|
+---+----------+-----------+--------+----+-------------------+
| 14|2010-01-01|       92.7|    Ajay|   1|2025-02-17 12:30:45|
| 15|2009-06-04|       88.5|Bharghav|   2|2025-02-17 12:35:30|
| 13|2010-12-12|       75.8| Chaitra|   3|2025-02-17 12:45:10|
| 14|2010-08-25|       82.3|   Kamal|   4|2025-02-17 12:40:05|
| 13|2009-04-14|       90.6|  Sohaib|   5|2025-02-17 12:55:20|
+---+----------+-----------+--------+----+-------------------+
```

### Summary
In this article, we have looked into
- How spark reads data that is in the date and timestamp format.
- Steps that will correctly parse the date and time format values.
- Ways to read date and timestamps of formats which are different from the standard format.

### References
- [Spark date time formats](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)
- [Spark JSON documentation](https://spark.apache.org/docs/3.5.4/sql-data-sources-json.html)
- [JSON data types](https://json-schema.org/understanding-json-schema/reference/numeric)
- [What is the "right" JSON date format?](https://stackoverflow.com/questions/10286204/what-is-the-right-json-date-format)