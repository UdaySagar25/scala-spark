### Value Access Window Functions

In the previous article, we have seen different way to rank the rows in a window partition which helps is organizing data.
Now, in this article, we will look into different window functions which can be used to retrieve data efficiently from a window partition.
To use these functions we need to import `sql.expressions.Window` class.

The functions we will explore today are
- lag()
- lead()
- nth_value()
- cume_dist()

We will be using the same dataframe that we used in the previous article.
```text
+---+--------+-----------+----------+-------------------+-----+
| ID|    Name|Room Number|       DOB|        Submit Time|Marks|
+---+--------+-----------+----------+-------------------+-----+
|  1|    Ajay|         10|2010-01-01|2025-02-17 12:30:45|92.75|
|  2|Bharghav|         20|2009-06-04|2025-02-17 08:15:30| 88.5|
|  3| Chaitra|         30|2010-12-12|2025-02-17 14:45:10| 75.8|
|  4|   Kamal|         20|2010-08-25|2025-02-17 17:10:05| 82.3|
|  5|  Sohaib|         30|2009-04-14|2025-02-17 09:55:20| 90.6|
|  6|  Tanish|         20|2009-05-11|2025-02-17 09:45:30| 88.5|
|  7|    Uday|         20|2009-09-06|2025-02-17 09:45:30| 92.3|
+---+--------+-----------+----------+-------------------+-----+
```

### What data is retrieved when `lag()` is used?
The lag() function in Spark looks at a column and grabs the value from a row that comes before the current row, based on a number (offset) you specify.
```scala
val lagRows=df.withColumn("Lag Rows", lag(col("Marks"),1,0).over(rankRow))
lagRows.show()
```
**Output**
```text
+---+--------+-----------+----------+-------------------+-----+--------+
| ID|    Name|Room Number|       DOB|        Submit Time|Marks|Lag Rows|
+---+--------+-----------+----------+-------------------+-----+--------+
|  4|   Kamal|         20|2010-08-25|2025-02-17 17:10:05| 82.3|     0.0|
|  2|Bharghav|         20|2009-06-04|2025-02-17 08:15:30| 88.5|    82.3|
|  6|  Tanish|         20|2009-05-11|2025-02-17 09:45:30| 88.5|    88.5|
|  7|    Uday|         20|2009-09-06|2025-02-17 09:45:30| 92.3|    88.5|
|  1|    Ajay|         10|2010-01-01|2025-02-17 12:30:45|92.75|     0.0|
|  3| Chaitra|         30|2010-12-12|2025-02-17 14:45:10| 75.8|     0.0|
|  5|  Sohaib|         30|2009-04-14|2025-02-17 09:55:20| 90.6|    75.8|
+---+--------+-----------+----------+-------------------+-----+--------+
```

In the output, you can see that the values of the previous rows are being returned in Lag Rows column when the offset value is set to 1.

### What values does `lead()` function return?
The lead() function in Spark looks at a column and grabs the value from a row that comes after the current row, based on a number (offset) you specify.
```scala
val leadRows=df.withColumn("Lag Rows", lead(col("Marks"),1,0).over(rankRow))
leadRows.show()
```
**Output**
```text
+---+--------+-----------+----------+-------------------+-----+---------+
| ID|    Name|Room Number|       DOB|        Submit Time|Marks|Lead Rows|
+---+--------+-----------+----------+-------------------+-----+---------+
|  4|   Kamal|         20|2010-08-25|2025-02-17 17:10:05| 82.3|     88.5|
|  2|Bharghav|         20|2009-06-04|2025-02-17 08:15:30| 88.5|     88.5|
|  6|  Tanish|         20|2009-05-11|2025-02-17 09:45:30| 88.5|     92.3|
|  7|    Uday|         20|2009-09-06|2025-02-17 09:45:30| 92.3|      0.0|
|  1|    Ajay|         10|2010-01-01|2025-02-17 12:30:45|92.75|      0.0|
|  3| Chaitra|         30|2010-12-12|2025-02-17 14:45:10| 75.8|     90.6|
|  5|  Sohaib|         30|2009-04-14|2025-02-17 09:55:20| 90.6|      0.0|
+---+--------+-----------+----------+-------------------+-----+---------+
```
In the output, you can see that the values of the ahead rows are being returned in Lead Rows column when the offset value is set to 1.

### What values does `cume_dist()` function return?
The cume_dist() function calculates the cumulative distribution of a value within a partition, returning a value between 0 and 1 representing the proportion of rows with values less than or equal to the current row’s value.


### Summary
In this article, we have seen:

### Related Articles
- [Understanding ranking window functions](@/docs/spark/understanding-ranking-window-functions)

### References
- [Using window functions in spark](https://stackoverflow.com/questions/36171349/using-windowing-functions-in-spark)
- [Window functions in Spark](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#window-functions)
