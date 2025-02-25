### Grouping Operations

Apache Sparks provides features for data aggregation which makes advance data analysis and data extraction much easier. 
Key features of data aggregation is **groupBy** and **grouping sets** which provide options/ways to aggregate data.

In this article, let us look at concepts of groupBy and grouping sets through practical examples.

### What is groupBy() operator?
groupBy() is a operator that is used to group data based on columns and apply aggregation functions.
We have already discussed various aggregation functions. To refresh your memory about aggregation, read [Mathematical Operations](@/docs/spark/advanced-mathematical-operations.md) article.

Consider the dataframe
```text
+--------+-----------+----------+-----+
|Item no.|  Item Name|  Category|  MRP|
+--------+-----------+----------+-----+
|       1|Paper Clips|Stationery|   23|
|       2|     Butter|     Dairy|   57|
|       3|      Jeans|   Clothes|  799|
|       4|      Shirt|   Clothes|  570|
|       5|Butter Milk|     Dairy|   50|
|       6|        Bag|   Apparel|  455|
|       7|      Shoes|   Apparel|  901|
|       8|    Stapler|Stationery|   50|
|       9|       Pens|Stationery|  120|
+--------+-----------+----------+-----+
```
Let us try to apply groupBy operator for calculating the total price, category wise.

```scala
val result=df.groupBy("Category").agg(sum("MRP").alias("Category wise total"))
result.show()
```
**Output**
```text
+----------+-------------------+
|  Category|Category wise total|
+----------+-------------------+
|   Apparel|               1356|
|Stationery|                193|
|     Dairy|                107|
|   Clothes|               1369|
+----------+-------------------+
```

Let us see how we can implement `count()` method.
```scala
val categoryCount=df.groupBy("Category").agg(count("Category").alias("Total number of items"))
    categoryCount.show(truncate = false)
```
**Output**
```text
+----------+----------------------+
|Category  |Total number of items |
+----------+----------------------+
|Apparel   |2                     |
|Stationery|3                     |
|Dairy     |2                     |
|Clothes   |2                     |
+----------+----------------------+
```
Similarly, we can apply other aggregation functions like, `avg()`, `min()`, `max()` etc.

### What are grouping sets?
Grouping sets are operators that allow users to implement multiple aggregations in one query.
Where we run different queries to implement multiple aggregations, using grouping sets, we can implement that as a consolidated multiple group-by operations.

Let us consider another dataframe to show the implementation of grouping sets.

```text
+--------+-----------+----------+---+------------------+-----------------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|
+--------+-----------+----------+---+------------------+-----------------+
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|
|       2|     Butter|     Dairy| 57|51.300000000000004|            61.56|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|
|       6|        Bag|   Apparel|455|             409.5|            491.4|
|       7|      Shoes|   Apparel|901|             810.9|973.0799999999999|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|
|       9|       Pens|Stationery|120|             108.0|            129.6|
+--------+-----------+----------+---+------------------+-----------------+
```
While the method `GROUPING SETS` is only available in SQL, spark provides similar operator that execute in the way similar to `GROUPING SETS`.
These operators are:
- rollup
- cube

### What is `rollup()` operator?
Rollup is a hierarchical aggregation operator, specializing in multidimensional aggregation, meaning, that it will calculate sub-totals in a given dataframe in an efficient way,
Let us see the implementation of `rollup()`
```scala
val rollupDf = priceAfterTax.rollup("Category","Item Name")
      .agg(sum("MRP").as("Total MRP"),
        sum("Discounted Price").as("Total Discounted Price"),
        sum("Price After Tax").as("Total Price After Tax")
      ).orderBy("Category","Item Name")

rollupDf.show()
```
**Output**
```text
+----------+-----------+---------+----------------------+---------------------+
|  Category|  Item Name|Total MRP|Total Discounted Price|Total Price After Tax|
+----------+-----------+---------+----------------------+---------------------+
|      null|       null|     3025|                2722.5|               3267.0|
|   Apparel|       null|     1356|                1220.4|              1464.48|
|   Apparel|        Bag|      455|                 409.5|                491.4|
|   Apparel|      Shoes|      901|                 810.9|    973.0799999999999|
|   Clothes|       null|     1369|                1232.1|              1478.52|
|   Clothes|      Jeans|      799|                 719.1|               862.92|
|   Clothes|      Shirt|      570|                 513.0|                615.6|
|     Dairy|       null|      107|     96.30000000000001|               115.56|
|     Dairy|     Butter|       57|    51.300000000000004|                61.56|
|     Dairy|Butter Milk|       50|                  45.0|                 54.0|
|Stationery|       null|      193|                 173.7|               208.44|
|Stationery|Paper Clips|       23|                  20.7|                24.84|
|Stationery|       Pens|      120|                 108.0|                129.6|
|Stationery|    Stapler|       50|                  45.0|                 54.0|
+----------+-----------+---------+----------------------+---------------------+
```
In the output, you can see that the category wise total and the total of all the items has been displayed in the same table.

### What is `cube()` operator?
The cube operator calculates the aggregate at all possible stages of the columns. Unlike `rollup()` which assumes hierarchical aggregation, `cube()` computes all possible combinations at every level.
This is a very useful operator as it aggregate the values from the ground level, giving the analyst a better summary.

Let us see an example on how to implement `cube()` operator
```scala
val cubeDf = priceAfterTax.cube("Category","Item Name")
      .agg(sum("MRP").as("Total MRP"),
        sum("Discounted Price").as("Total Discounted Price"),
        sum("Price After Tax").as("Total Price After Tax")
      ).orderBy("Category","Item Name")

cubeDf.show()
```

**Output**
```text
+----------+-----------+---------+----------------------+---------------------+
|  Category|  Item Name|Total MRP|Total Discounted Price|Total Price After Tax|
+----------+-----------+---------+----------------------+---------------------+
|      null|       null|     3025|                2722.5|               3267.0|
|      null|        Bag|      455|                 409.5|                491.4|
|      null|     Butter|       57|    51.300000000000004|                61.56|
|      null|Butter Milk|       50|                  45.0|                 54.0|
|      null|      Jeans|      799|                 719.1|               862.92|
|      null|Paper Clips|       23|                  20.7|                24.84|
|      null|       Pens|      120|                 108.0|                129.6|
|      null|      Shirt|      570|                 513.0|                615.6|
|      null|      Shoes|      901|                 810.9|    973.0799999999999|
|      null|    Stapler|       50|                  45.0|                 54.0|
|   Apparel|       null|     1356|                1220.4|              1464.48|
|   Apparel|        Bag|      455|                 409.5|                491.4|
|   Apparel|      Shoes|      901|                 810.9|    973.0799999999999|
|   Clothes|       null|     1369|                1232.1|              1478.52|
|   Clothes|      Jeans|      799|                 719.1|               862.92|
|   Clothes|      Shirt|      570|                 513.0|                615.6|
|     Dairy|       null|      107|     96.30000000000001|               115.56|
|     Dairy|     Butter|       57|    51.300000000000004|                61.56|
|     Dairy|Butter Milk|       50|                  45.0|                 54.0|
|Stationery|       null|      193|                 173.7|               208.44|
+----------+-----------+---------+----------------------+---------------------+
```

As discussed earlier, in the output we can see that the aggregation is done from the root level, i.e., aggregate is done on every individual item, then every category and then on the whole dataframe.

**Note:** Using `cube()` operation on big data is not advisable as it computationally expensive it maybe hard to read. It is suggested to use `rollup()` or `groupBy()` when you have big data.

### Summary
In this article, we have seen:
- GroupBy operator and how it is used along with aggregation functions.
- What grouping sets are and different types of grouping sets
- Use case of `rollup()` operator.
- Use case of `cube()` operator.

### Related Articles
- [Mathematical Operations](@/docs/spark/advanced-mathematical-operations.md)

### References
- [Spark Dataframes groupBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html)
- [What is the difference between cube, rollup and groupBy operators?](https://stackoverflow.com/questions/37975227/what-is-the-difference-between-cube-rollup-and-groupby-operators)
- [GROUPING SETS, CUBE, and ROLLUP](https://www.postgresql.org/docs/9.5/queries-table-expressions.html#QUERIES-GROUPING-SETS)