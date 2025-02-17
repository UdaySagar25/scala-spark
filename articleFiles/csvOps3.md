## Handle Date and Time format values in a csv field using spark

created on: 17-2-2025

There are many instances where we make note of the time of creating a record of data. Few follow the standard notation of date and time format
Few others follow a custom way of recording the date and time. To handle these kind of situations, spark has few methods which help us in correctly interpreting the date and time field.


Let us now look at the scenario of handling data with different date and time format.

**Example csv file:** 

**File name:** studentDate.csv
```csv
ID,Name,LongValue,DOB, Submit Time,DoubleMarks
1,"Ajay",10,"2010:01:01","2025-02-17 12:30:45",92.75
2,"Bharghav",20,"2009:06:04","2025-02-17 08:15:30",88.5
3,"Chaitra",30,"2010:12:12","2025-02-17 14:45:10",75.8
4,"Kamal",40,"2010:08:25","2025-02-17 17:10:05",82.3
5,"Sohaib",50,"2009:04:14","2025-02-17 09:55:20",90.6
```

### How does spark read the csv file if the schema is not inferred or not defined?
Usually, by default, if schema is not defined, spark reads all the columns as String.
```scala
val dateDf=spark.read.option("header","true")
      .csv("csvFiles/studentDate.csv")

println("DF-1")
dateDf.show()
dateDf.printSchema()
```
**Output**
```text
DF-1
+---+--------+---------+----------+-------------------+-----------+
| ID|    Name|LongValue|       DOB|        Submit Time|DoubleMarks|
+---+--------+---------+----------+-------------------+-----------+
|  1|    Ajay|       10|2010:01:01|2025-02-17 12:30:45|      92.75|
|  2|Bharghav|       20|2009:06:04|2025-02-17 08:15:30|       88.5|
|  3| Chaitra|       30|2010:12:12|2025-02-17 14:45:10|       75.8|
|  4|   Kamal|       40|2010:08:25|2025-02-17 17:10:05|       82.3|
|  5|  Sohaib|       50|2009:04:14|2025-02-17 09:55:20|       90.6|
+---+--------+---------+----------+-------------------+-----------+

root
 |-- ID: string (nullable = true)
 |-- Name: string (nullable = true)
 |-- LongValue: string (nullable = true)
 |-- DOB: string (nullable = true)
 |--  Submit Time: string (nullable = true)
 |-- DoubleMarks: string (nullable = true)
```
The above output is of no use to us, as all the necessary is inferred as String, which will make it difficult with further analysis.

To tackle this, We make sure that spark infers the schema automatically. You can refer the [Handling schema of a csv file](csvOps2.md) on how to use the flag `inferSchema` to define the schema of a csv file.

## How to correctly define the data type of date and time stamp column in csv file?

Spark has few methods, which will help us in correctly defining the date and time format in a csv file.
We shall now look at them and also at the behaviour of the schema for the method.

```scala
val dateDf1=spark.read.option("header","true")
      .option("inferSchema","true")
      .option("dateFormat","yyyy-MM-dd")
      .csv("csvFiles/studentDate.csv")

println("DF-2")
dateDf1.show()
dateDf1.printSchema()
```
We can use the spark's `dateFormat` flag to specifically set the date format.
How do you think the schema will be inferred as, based on the above code and csv data?
```text
DF-2
+---+--------+---------+----------+-------------------+-----------+
| ID|    Name|LongValue|       DOB|        Submit Time|DoubleMarks|
+---+--------+---------+----------+-------------------+-----------+
|  1|    Ajay|       10|2010:01:01|2025-02-17 12:30:45|      92.75|
|  2|Bharghav|       20|2009:06:04|2025-02-17 08:15:30|       88.5|
|  3| Chaitra|       30|2010:12:12|2025-02-17 14:45:10|       75.8|
|  4|   Kamal|       40|2010:08:25|2025-02-17 17:10:05|       82.3|
|  5|  Sohaib|       50|2009:04:14|2025-02-17 09:55:20|       90.6|
+---+--------+---------+----------+-------------------+-----------+

root
 |-- ID: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- LongValue: integer (nullable = true)
 |-- DOB: string (nullable = true)
 |--  Submit Time: timestamp (nullable = true)
 |-- DoubleMarks: double (nullable = true)
```
We can clearly see that the **DOB** column's schema behaves a bit different as the format we have defined in the code is different from what is there in csv file. 
So, how to handle this?

### How to define our custom format for date type fields?
In the flag `option("dateFormat","yyyy-MM-dd")`, we can make changes to format accordingly. In our case, it will be `option("dateFormat","yyyy:MM:dd")`
Therefore, the updated code would be:
```scala
val dateDf2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "yyyy:MM:dd")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("csvFiles/studentDate.csv")

println("Updated CSV File")
dateDf2.show()
dateDf2.printSchema()
```
And the corresponding output would be 
**Output**
```text
Updated CSV File
+---+--------+---------+----------+-------------------+-----------+
| ID|    Name|LongValue|       DOB|        Submit Time|DoubleMarks|
+---+--------+---------+----------+-------------------+-----------+
|  1|    Ajay|       10|2010-01-01|2025-02-17 12:30:45|      92.75|
|  2|Bharghav|       20|2009-06-04|2025-02-17 08:15:30|       88.5|
|  3| Chaitra|       30|2010-12-12|2025-02-17 14:45:10|       75.8|
|  4|   Kamal|       40|2010-08-25|2025-02-17 17:10:05|       82.3|
|  5|  Sohaib|       50|2009-04-14|2025-02-17 09:55:20|       90.6|
+---+--------+---------+----------+-------------------+-----------+

root
 |-- ID: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- LongValue: integer (nullable = true)
 |-- DOB: date (nullable = true)
 |--  Submit Time: timestamp (nullable = true)
 |-- DoubleMarks: double (nullable = true)
```

Now assume that we have another data file with different style of date and time entry.

csv file:
```csv
ID,Name,LongValue,DOB,Submit Time,DoubleMarks
1,"Ajay",10,"01/01/2010","2025/02/17 12:30:45",92.75
2,"Bharghav",20,"04/06/2009","2025/02/17 12:35:30",88.5
3,"Chaitra",30,"12/12/2010","2025/02/17 12:45:10",75.8
4,"Kamal",40,"25/08/2010","2025/02/17 12:40:05",82.3
5,"Sohaib",50,"14/04/2009","2025/02/17 12:55:20",90.6
```
Here we can see that the style of **DOB** column and **Submit Time** column are differently recorded.

### How to deal with csv files having date and timestamp format differently?

Like the previous, scenario, here also we have the same issue with date and timestamp format. We will use the following flags, `dateFormat` and `timestampFormat` to enable spark read csv files.
```scala
val dateDf3 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "dd-MM-yyyy")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("csvFiles/studentDate2.csv")

println("New CSV file")
dateDf3.show()
dateDf3.printSchema()
```
**Output**
```text
New CSV file
+---+--------+---------+----------+-------------------+-----------+
| ID|    Name|LongValue|       DOB|        Submit Time|DoubleMarks|
+---+--------+---------+----------+-------------------+-----------+
|  1|    Ajay|       10|2010-01-01|2025-02-17 12:30:45|      92.75|
|  2|Bharghav|       20|2009-06-04|2025-02-17 12:35:30|       88.5|
|  3| Chaitra|       30|2010-12-12|2025-02-17 12:45:10|       75.8|
|  4|   Kamal|       40|2010-08-25|2025-02-17 12:40:05|       82.3|
|  5|  Sohaib|       50|2009-04-14|2025-02-17 12:55:20|       90.6|
+---+--------+---------+----------+-------------------+-----------+

root
 |-- ID: integer (nullable = true)
 |-- Name: string (nullable = true)
 |-- LongValue: integer (nullable = true)
 |-- DOB: date (nullable = true)
 |-- Submit Time: timestamp (nullable = true)
 |-- DoubleMarks: double (nullable = true)
```

As we can see, spark is able to read the csv file with different date and timestamp formats


### Summary
- In this article, we have seen how to handle data of different date and timestamps formats.
- We have also understood the behaviour of data if the wrong format is given.

### Related Articles
- [Handling schema of a csv file](csvOps2.md)

### References
- [CSV Spark documentation](https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html)
- [Read CSV with Spark](https://stackoverflow.com/questions/38647132/read-csv-with-spark)
- [Reading CSV into a Spark Dataframe with timestamp and date types](https://stackoverflow.com/questions/40878243/reading-csv-into-a-spark-dataframe-with-timestamp-and-date-types)
- [Convert a string column to timestamp when read into spark](https://stackoverflow.com/questions/57705211/convert-a-string-column-to-timestamp-when-read-into-spark)
