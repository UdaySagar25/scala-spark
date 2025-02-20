# Spark DateTime Datatype

created on: 11-2-2025

First let us create a dataframe with datetime data type.
`.todate()` method can be used to convert a String type to DateTime type in a dataframe
```scala
val df = studentData.toDF("Roll", "Name", "Marks", "EnrollmentDate")
val dfWithDate = df.withColumn("EnrollmentDate", to_date($"EnrollmentDate"))
dfWithDate.show()
```

```text
+----+--------+-----+--------------+
|Roll|    Name|Marks|EnrollmentDate|
+----+--------+-----+--------------+
|   1|    Ajay|   55|    2024-09-15|
|   2|Bharghav|   63|    2024-09-10|
|   3| Chaitra|   60|    2024-09-20|
|   4|   Kamal|   75|    2024-09-12|
|   5|  Sohaib|   70|    2024-09-18|
+----+--------+-----+--------------+
```

### How to extract year, month and day from a spark dataframe?
you can use a combination of methods to extract the year, month and day from the spark dataframe.
```scala
val dfWithComponents = dfWithDate.withColumn("EnrollmentYear", year($"EnrollmentDate"))
      .withColumn("EnrollmentMonth", month($"EnrollmentDate"))
      .withColumn("EnrollmentDay", dayofmonth($"EnrollmentDate"))

dfWithComponents.show()
```
**Output**
```text
+----+--------+-----+--------------+--------------+---------------+-------------+
|Roll|    Name|Marks|EnrollmentDate|EnrollmentYear|EnrollmentMonth|EnrollmentDay|
+----+--------+-----+--------------+--------------+---------------+-------------+
|   1|    Ajay|   55|    2024-09-15|          2024|              9|           15|
|   2|Bharghav|   63|    2024-09-10|          2024|              9|           10|
|   3| Chaitra|   60|    2024-09-20|          2024|              9|           20|
|   4|   Kamal|   75|    2024-09-12|          2024|              9|           12|
|   5|  Sohaib|   70|    2024-09-18|          2024|              9|           18|
+----+--------+-----+--------------+--------------+---------------+-------------+
```

### How to extract the details about day of the year, week of the year and which quarter of the year?
Extracting this kind of information is quite similar to extracting the details like extracting the year, month, and day.
```scala
val moreComponents=dfWithDate.withColumn("DayOfYear", dayofyear($"EnrollmentDate"))
      .withColumn("WeekOfYear", weekofyear($"EnrollmentDate"))
      .withColumn("Quarter", quarter($"EnrollmentDate"))
moreComponents.show()
```
**Output**
```text
+----+--------+-----+--------------+---------+----------+-------+
|Roll|    Name|Marks|EnrollmentDate|DayOfYear|WeekOfYear|Quarter|
+----+--------+-----+--------------+---------+----------+-------+
|   1|    Ajay|   55|    2024-09-15|      259|        37|      3|
|   2|Bharghav|   63|    2024-09-10|      254|        37|      3|
|   3| Chaitra|   60|    2024-09-20|      264|        38|      3|
|   4|   Kamal|   75|    2024-09-12|      256|        37|      3|
|   5|  Sohaib|   70|    2024-09-18|      262|        38|      3|
+----+--------+-----+--------------+---------+----------+-------+
```

### How to format dates?
Formatting date is very essentail as it will be helpful to users in understanding the data who are from different time zones and follow different date format.
We use the method `date_format()` to change the date format.
```scala
val dfFormatted = dfWithDate.withColumn("FormattedDate", date_format($"EnrollmentDate", "yyyy-MM-dd")) // ISO 8601 format
      .withColumn("AnotherFormat", date_format($"EnrollmentDate", "MM/dd/yyyy")) // US format
      .withColumn("MonthName", date_format($"EnrollmentDate", "MMMM")) // Full month name

dfFormatted.show()
```
**Output**
```text
+----+--------+-----+--------------+-------------+-------------+---------+
|Roll|    Name|Marks|EnrollmentDate|FormattedDate|AnotherFormat|MonthName|
+----+--------+-----+--------------+-------------+-------------+---------+
|   1|    Ajay|   55|    2024-09-15|   2024-09-15|   09/15/2024|September|
|   2|Bharghav|   63|    2024-09-10|   2024-09-10|   09/10/2024|September|
|   3| Chaitra|   60|    2024-09-20|   2024-09-20|   09/20/2024|September|
|   4|   Kamal|   75|    2024-09-12|   2024-09-12|   09/12/2024|September|
|   5|  Sohaib|   70|    2024-09-18|   2024-09-18|   09/18/2024|September|
+----+--------+-----+--------------+-------------+-------------+---------+
```

### How to get the current timestamp ?
To display the current timestamp, we can use the method `current_timestamp()`.
```scala
val dfWithTimestamp = dfWithDate.withColumn("RegistrationTime", current_timestamp())
dfWithTimestamp.show()
```
**Output**
```text
+----+--------+-----+--------------+--------------------+
|Roll|    Name|Marks|EnrollmentDate|    RegistrationTime|
+----+--------+-----+--------------+--------------------+
|   1|    Ajay|   55|    2024-09-15|2025-02-11 16:09:...|
|   2|Bharghav|   63|    2024-09-10|2025-02-11 16:09:...|
|   3| Chaitra|   60|    2024-09-20|2025-02-11 16:09:...|
|   4|   Kamal|   75|    2024-09-12|2025-02-11 16:09:...|
|   5|  Sohaib|   70|    2024-09-18|2025-02-11 16:09:...|
+----+--------+-----+--------------+--------------------+
```
### How to get timestamp of a different time zone ?
To get the timestamp of a different time zone, we have to use a combination of methods. `from_utc_timestamp()` is used inside `.withColumn()` to get the different time stamp.
```scala
val dfWithTimeZone = dfWithTimestamp.withColumn("ISTRegistrationTime", from_utc_timestamp($"RegistrationTime", "Asia/Kolkata"))
dfWithTimeZone.show()
```
**Output**
```text
+----+--------+-----+--------------+--------------------+--------------------+
|Roll|    Name|Marks|EnrollmentDate|    RegistrationTime| ISTRegistrationTime|
+----+--------+-----+--------------+--------------------+--------------------+
|   1|    Ajay|   55|    2024-09-15|2025-02-12 16:09:...|2025-02-12 21:39:...|
|   2|Bharghav|   63|    2024-09-10|2025-02-12 16:09:...|2025-02-12 21:39:...|
|   3| Chaitra|   60|    2024-09-20|2025-02-12 16:09:...|2025-02-12 21:39:...|
|   4|   Kamal|   75|    2024-09-12|2025-02-12 16:09:...|2025-02-12 21:39:...|
|   5|  Sohaib|   70|    2024-09-18|2025-02-12 16:09:...|2025-02-12 21:39:...|
+----+--------+-----+--------------+--------------------+--------------------+
```

### Summary 
Through this article, we have got the experience to work with DateTime Types of spark dataframe.
We have covered the topics,
- Create a dataframe with datetime type
- Extract the year, month and day from the dataframe
- Extract more information about the datetime column of the dataframe
- Formatting the Dates column
- How to get the current time stamps

### References
- [Dataframe date format](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_format.html)
- [Dates and Timestamps in apache spark](https://www.databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html)
