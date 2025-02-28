## Understanding Ranking Window Functions

Window functions are used to perform advanced data analysis and calculations across the dataframe records.
Window functions enable calculations that depend on the context of other rows or calculating cumulative metrics. The specialty of window functions is that it is primarily defined to handle big data efficiently.

To work with window functions, we have to import `sql.expressions.Window` class in the spark session.

In this article, we shall be discussing ranking functions. These functions are widely used to assign ranks or group rows based on their order.
We will see the following ranking functions.
- rank()
- dense_rank()
- percent_rank()
- row_number()

Consider the dataframe
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

### How to rank the records of the dataframe?
To assign ranks to the records of the given dataframe, we use `rank()` function, which assigns rank to each row. If there are duplicate rows then same rank is given to them and the next rank is skipped.
```scala
val rankRow = Window.partitionBy(col("Room Number")).orderBy(col("Marks"))

val result = df.withColumn("rank", rank().over(rankRow))
result.show()
```
**Output**
```text
+---+--------+-----------+----------+-------------------+-----+----+
| ID|    Name|Room Number|       DOB|        Submit Time|Marks|rank|
+---+--------+-----------+----------+-------------------+-----+----+
|  4|   Kamal|         20|2010-08-25|2025-02-17 17:10:05| 82.3|   1|
|  2|Bharghav|         20|2009-06-04|2025-02-17 08:15:30| 88.5|   2|
|  6|  Tanish|         20|2009-05-11|2025-02-17 09:45:30| 88.5|   2|
|  7|    Uday|         20|2009-09-06|2025-02-17 09:45:30| 92.3|   4|
|  1|    Ajay|         10|2010-01-01|2025-02-17 12:30:45|92.75|   1|
|  3| Chaitra|         30|2010-12-12|2025-02-17 14:45:10| 75.8|   1|
|  5|  Sohaib|         30|2009-04-14|2025-02-17 09:55:20| 90.6|   2|
+---+--------+-----------+----------+-------------------+-----+----+
```
In the above output dataframe, rows that have same room number value are clubbed into one window and the rows are being ranked accordingly.

### How does dense_rank() work?
Similar to `rank()`, `dense_rank()` assigns ranks but does not leave gaps between ranks in case of ties. The next distinct value gets the next consecutive rank.
```scala
val denseResult=df.withColumn("rank",dense_rank().over(rankRow))
denseResult.show()
```
**Output**
```text
+---+--------+-----------+----------+-------------------+-----+----+
| ID|    Name|Room Number|       DOB|        Submit Time|Marks|rank|
+---+--------+-----------+----------+-------------------+-----+----+
|  4|   Kamal|         20|2010-08-25|2025-02-17 17:10:05| 82.3|   1|
|  2|Bharghav|         20|2009-06-04|2025-02-17 08:15:30| 88.5|   2|
|  6|  Tanish|         20|2009-05-11|2025-02-17 09:45:30| 88.5|   2|
|  7|    Uday|         20|2009-09-06|2025-02-17 09:45:30| 92.3|   3|
|  1|    Ajay|         10|2010-01-01|2025-02-17 12:30:45|92.75|   1|
|  3| Chaitra|         30|2010-12-12|2025-02-17 14:45:10| 75.8|   1|
|  5|  Sohaib|         30|2009-04-14|2025-02-17 09:55:20| 90.6|   2|
+---+--------+-----------+----------+-------------------+-----+----+
```
If you compare the results of rank() and dense_rank(), you can see the difference between both functions.

### What is the use of `percent_rank()`?
The percent_rank() function calculates the relative rank of a row within its partition as a value between 0 and 1.
It’s calculated as (rank-1)/(total rows in partition-1)
```scala
val percentRank=df.withColumn("Percent Rank",percent_rank().over(rankRow))
percentRank.show()
```
**Output**
```text
+---+--------+-----------+----------+-------------------+-----+------------------+
| ID|    Name|Room Number|       DOB|        Submit Time|Marks|      Percent Rank|
+---+--------+-----------+----------+-------------------+-----+------------------+
|  4|   Kamal|         20|2010-08-25|2025-02-17 17:10:05| 82.3|               0.0|
|  2|Bharghav|         20|2009-06-04|2025-02-17 08:15:30| 88.5|0.3333333333333333|
|  6|  Tanish|         20|2009-05-11|2025-02-17 09:45:30| 88.5|0.3333333333333333|
|  7|    Uday|         20|2009-09-06|2025-02-17 09:45:30| 92.3|               1.0|
|  1|    Ajay|         10|2010-01-01|2025-02-17 12:30:45|92.75|               0.0|
|  3| Chaitra|         30|2010-12-12|2025-02-17 14:45:10| 75.8|               0.0|
|  5|  Sohaib|         30|2009-04-14|2025-02-17 09:55:20| 90.6|               1.0|
+---+--------+-----------+----------+-------------------+-----+------------------+
```

### What does row_number() do?
The `row_number()` simply assigns a sequential value to the row, irrespective of duplicates. This return the rows with the row number.
```scala
val rowNumber=df.withColumn("Row Number", row_number().over(rankRow))
rowNumber.show()
```
**Output**
```text
+---+--------+-----------+----------+-------------------+-----+----------+
| ID|    Name|Room Number|       DOB|        Submit Time|Marks|Row Number|
+---+--------+-----------+----------+-------------------+-----+----------+
|  4|   Kamal|         20|2010-08-25|2025-02-17 17:10:05| 82.3|         1|
|  2|Bharghav|         20|2009-06-04|2025-02-17 08:15:30| 88.5|         2|
|  6|  Tanish|         20|2009-05-11|2025-02-17 09:45:30| 88.5|         3|
|  7|    Uday|         20|2009-09-06|2025-02-17 09:45:30| 92.3|         4|
|  1|    Ajay|         10|2010-01-01|2025-02-17 12:30:45|92.75|         1|
|  3| Chaitra|         30|2010-12-12|2025-02-17 14:45:10| 75.8|         1|
|  5|  Sohaib|         30|2009-04-14|2025-02-17 09:55:20| 90.6|         2|
+---+--------+-----------+----------+-------------------+-----+----------+
```

### Summary
In this article, we have seen:
- What window functions are and why they are used.
- Different types of window functions and their use case.

### Related Articles
- [Sort Functions](@/docs/spark/sort-functions.md)
- [Sorting Records with Null Values](@/docs/spark/sorting-records-with-null-values.md)

###  References
- [Window Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#window-functions)
- [Introducing Window Functions in Spark](https://www.databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html)
