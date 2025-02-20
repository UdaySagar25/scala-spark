## Math Operations 

created on: 12-2-2025

In this article, we are going to see different math operations and its application on a dataframes.
These math operations generally include methods like, addition, subtraction, multiplication, division, average, floor and ceil values and many more.
Let us now look into each of these mathematical operations and how they are applied on dataframes.

We'll be using the below dataframe which we already created for DataType operations [Refer this link on creating a dataframe](datatypes.md)

```text
+----+--------+-----------+-----------+------------+
|Roll|    Name|Final Marks|Float Marks|Double Marks|
+----+--------+-----------+-----------+------------+
|   1|    Ajay|        300|       55.5|       92.75|
|   2|Bharghav|        350|       63.2|        88.5|
|   3| Chaitra|        320|       60.1|        75.8|
|   4|   Kamal|        360|       75.0|        82.3|
|   5|  Sohaib|        450|       70.8|        90.6|
+----+--------+-----------+-----------+------------+
```
Mathematical operations are widely divided into 2 types
- Arithmetic Operations
- Aggregate Operations

First we will look into Arithmetics Operations
### How to find the sum of two columns?
Addition of column values of a dataframe is processed through `.withColumn()` along with other parameters.
```scala
val sumCol=df.withColumn("Sum of scores", col("Float Marks")+col("Double Marks"))
sumCol.show()
```
**Output**
```text
+----+--------+-----------+-----------+------------+------------------+
|Roll|    Name|Final Marks|Float Marks|Double Marks|     Sum of scores|
+----+--------+-----------+-----------+------------+------------------+
|   1|    Ajay|        300|       55.5|       92.75|            148.25|
|   2|Bharghav|        350|       63.2|        88.5|151.70000076293945|
|   3| Chaitra|        320|       60.1|        75.8| 135.8999984741211|
|   4|   Kamal|        360|       75.0|        82.3|             157.3|
|   5|  Sohaib|        450|       70.8|        90.6| 161.4000030517578|
+----+--------+-----------+-----------+------------+------------------+
```

### How to find the difference between two columns?
Difference between two columns is calculated in a similar way the addition of values of two columns is done, i.e., using `.withColumn()` method
```scala
val diffCol=df.withColumn("Difference of scores", col("Double Marks")-col("Float Marks"))
diffCol.show()
```
**Output** 
```text
+----+--------+-----------+-----------+------------+------------------+
|Roll|    Name|Final Marks|Float Marks|Double Marks|     Sum of scores|
+----+--------+-----------+-----------+------------+------------------+
|   1|    Ajay|        300|       55.5|       92.75|             37.25|
|   2|Bharghav|        350|       63.2|        88.5|25.299999237060547|
|   3| Chaitra|        320|       60.1|        75.8|15.700001525878903|
|   4|   Kamal|        360|       75.0|        82.3| 7.299999999999997|
|   5|  Sohaib|        450|       70.8|        90.6|19.799996948242182|
+----+--------+-----------+-----------+------------+------------------+
```

### How to multiply the column values in a dataframe?
Mutliplication of column values is done in the similar how addition and subtraction was done earlier, using `.withColumn()`
```scala
val product=df.withColumn("Updated scores", col("Float Marks")*1.5)
product.show()
```
**Output**
```text
+----+--------+-----------+-----------+------------+------------------+
|Roll|    Name|Final Marks|Float Marks|Double Marks|    Updated scores|
+----+--------+-----------+-----------+------------+------------------+
|   1|    Ajay|        300|       55.5|       92.75|             83.25|
|   2|Bharghav|        350|       63.2|        88.5| 94.80000114440918|
|   3| Chaitra|        320|       60.1|        75.8| 90.14999771118164|
|   4|   Kamal|        360|       75.0|        82.3|             112.5|
|   5|  Sohaib|        450|       70.8|        90.6|106.20000457763672|
+----+--------+-----------+-----------+------------+------------------+
```

### How to apply divide operation to the dataframe?
Division of column values of a dataframe is carried out by the `/` operator and is applied within `.withColumn()` method
```scala
val division=df.withColumn("Updated scores", col("Final Marks")/2)
division.show()
```
**Output**
```text
+----+--------+-----------+-----------+------------+--------------+
|Roll|    Name|Final Marks|Float Marks|Double Marks|Updated scores|
+----+--------+-----------+-----------+------------+--------------+
|   1|    Ajay|        300|       55.5|       92.75|         150.0|
|   2|Bharghav|        350|       63.2|        88.5|         175.0|
|   3| Chaitra|        320|       60.1|        75.8|         160.0|
|   4|   Kamal|        360|       75.0|        82.3|         180.0|
|   5|  Sohaib|        450|       70.8|        90.6|         225.0|
+----+--------+-----------+-----------+------------+--------------+
```
This sums up the basic arithmetics operations, Now let us dive into aggregate operations.

Aggregate operations usually include, 
- sum() - Computes the sum of all the values in a dataframe column
- avg() - Computes the average of all the values in a dataframe column
- min() - Returns the minimum value of a column in a dataframe
- max() - Returns the maximum value of a column in a dataframe

### How to find the sum of values in a column?
We can use spark's `sum()` method inside the `.agg()` method 
```scala
val colSum=df.agg(sum("Final Marks").alias("Total Score"))
colSum.show()
```
**Output**
```text
+-----------+
|Total Score|
+-----------+
|       1780|
+-----------+
```

### How to find the average of column values?
We can use spark's `avg()` method inside the `.agg()` method
```scala
val colAvg=df.agg(avg("Float Marks").alias("Average Score"))
colAvg.show()
```
**Output**
```text
+-----------------+
|    Average Score|
+-----------------+
|64.92000045776368|
+-----------------+
```

### How to find the minimum value of a column in a dataframe?
We can use spark's `min()` method inside the `.agg()` method
```scala
val colMin=df.agg(min("Double Marks").alias("Minimum Score"))
colMin.show()
```
**Output**
```text
+-------------+
|Minimum Score|
+-------------+
|         75.8|
+-------------+
```
### How to find the maximum value of a column in a dataframe?
We can use spark's `max()` method inside the `.agg()` method
```scala
val colMax=df.agg(max("Float Marks").alias("Maximum Score"))
colMax.show()
```
**Output**
```text
+-------------+
|Maximum Score|
+-------------+
|         75.0|
+-------------+
```

### Summary
We have looked into various Mathematical Operations in a Spark Dataframe. We have covers basic arithmetic operations, using `.withColumn()`
and also saw how aggregate functions are applied using `.agg()` method inside which, we apply various aggregation methods.

### Related Articles
- [Creating a Dataframe](datatypes.md)

### References
- [Spark Math Operations](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#math-functions)
- [Statistical and Mathematical functions in Spark Dataframes](https://www.databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html)
