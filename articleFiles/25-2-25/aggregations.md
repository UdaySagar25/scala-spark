## Aggregations

In the previous articles, we have seen different mathematical functions from basics to advanced and how they are useful while dealing with real time data.
Now we extend our discussion to a next level, where we will be talking about aggregate functions and how these operations are useful.

Aggregate functions are used to summarize the large datasets efficiently, giving a single result. Since we are going to use Spark to handle large datasets, aggregate functions will be useful.

In this article we are going to explore Spark aggregate functions.
- sum() - Computes the sum of all the values of the selected column
- avg() - Computes the average of all the values of the selected column
- min() - Returns the minimum value of a selected column 
- max() - Returns the maximum value of a selected column 
- count() - Returns the number of items in a group
- variance() - Returns the variance of the selected column

For our discussion, we are going to use the below dataframe
```text
+----+--------+-----------+
|Roll|    Name|Final Marks|
+----+--------+-----------+
|   1|    Ajay|        300|
|   2|Bharghav|        350|
|   3| Chaitra|        320|
|   4|   Kamal|        360|
|   5|  Sohaib|        450|
|   6|    Neha|        390|
|   7|  Ramesh|        410|
|   8|   Sneha|        280|
|   9|   Varun|        330|
|  10|   Pooja|        370|
+----+--------+-----------+
```

### How to find the sum of values in a column?
We can use spark's `sum()` method inside the `.agg()` method
```scala
df.agg(sum("Final Marks").alias("Total Score")).show()
```
**Output**
```text
+-----------+
|Total Score|
+-----------+
|       3560|
+-----------+
```
### How to find the average of column values?
We can use spark's `avg()` method inside the `.agg()` method
```scala
df.agg(avg("Final Marks").alias("Average Score")).show()
```
**Output**
```text
+-------------+
|Average Score|
+-------------+
|        356.0|
+-------------+
```
### How to find the minimum value of a column in a dataframe?
We can use spark's `min()` method inside the `.agg()` method
```scala
df.agg(min("Final Marks").alias("Minimum Score")).show()
```
**Output**
```text
+-------------+
|Minimum Score|
+-------------+
|          280|
+-------------+
```
### How to find the minimum value of a column in a dataframe?
We can use spark's `max()` method inside the `.agg()` method
```scala
df.agg(avg("Final Marks").alias("Maximum Score")).show()
```
**Output**
```text
+-------------+
|Maximum Score|
+-------------+
|          450|
+-------------+
```

### How to find the total number of records in the dataframe?
We can use the `count()` function which will return the total number of records
```scala
df.agg(count("Name").alias("Total Students")).show()
```
**Output**
```text
+--------------+
|Total Students|
+--------------+
|            10|
+--------------+
```

### How to find the variance of a column?
To find the variance of column values, we can use `variance()` function.
```scala
df.agg(variance(col("Final Marks")).alias("Variance of Marks")).show()
```
**Output**
```text
+------------------+
| Variance of Marks|
+------------------+
|2671.1111111111118|
+------------------+
```

Consider the below dataframe
```text
+----+--------+-----------+---------+
|Roll|    Name|Final Marks|Unit Test|
+----+--------+-----------+---------+
|   1|    Ajay|        300|       49|
|   2|Bharghav|        350|       65|
|   3| Chaitra|        320|       48|
|   4|   Kamal|        360|       70|
|   5|  Sohaib|        450|       28|
|   6|    Neha|        390|       49|
|   7|  Ramesh|        410|       77|
|   8|   Sneha|        280|       38|
|   9|   Varun|        330|       47|
|  10|   Pooja|        370|       99|
+----+--------+-----------+---------+
```
### How to find the correlation between two column values?
Let us find the correlation between Final Marks and Unit Test marks using `corr()` function
```scala
updatedDf.agg(corr(col("Final Marks"),col("Unit Test")).alias("Correlation")).show()
```
**Output**
```text
+-----------+
|Correlation|
+-----------+
|-0.24483685|
+-----------+
```
Correlation is the value given to the relation between two attributes, and it ranges from -1 to +1, where -1 being negatively related, +1 being positively related and 0 being no related.


### Summary
In this article, we have seen:
- What aggregate functions are and how they are useful.
- Different aggregate functions that can be used on real time data.
- Implementation of aggregate function using `agg()` method.

### Related Articles
- [Mathematical Operations](@/docs/spark/mathematical-operations.md)
- [Advanced Mathematical Operations](@/docs/spark/advanced-mathematical-operations.md)

### References
- [Aggregation Operations](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#aggregate-functions)
- [Spark dataframe aggregation scala](https://stackoverflow.com/questions/42706014/spark-dataframe-aggregation-scala?rq=2)
- [Apache spark agg() function](https://stackoverflow.com/questions/43292947/apache-spark-agg-function)