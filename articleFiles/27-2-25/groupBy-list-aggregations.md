### GroupBy list aggregations

In our previous discussions, we have talked about Group By operations and how they are used along with aggregate functions.
Spark allows us to aggregate the data into list, making it easier for analysis.

List aggregations combine the outputs of all the aggregations into a single array, rather than returning scalar values. To do this,
we will be using Spark's `collect_list()` and `collect_set()` functions.
- `collect_list()` gathers all the values in a group into a list.
- `collect_set()` gathers all the unique values in a group to a list.

Let us use the dataframe that we have used during out discussion in [Grouping Operations](@/docs/spark/grouping-operations.md)
```text
+--------+-----------+----------+---+------------------+-----------------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|
+--------+-----------+----------+---+------------------+-----------------+
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|
|       2|     Butter|     Dairy| 57|     51.3000000004|            61.56|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|
|       6|        Bag|   Apparel|455|             409.5|            491.4|
|       7|      Shoes|   Apparel|901|             810.9|    973.079999999|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|
|       9|       Pens|Stationery|120|             108.0|            129.6|
+--------+-----------+----------+---+------------------+-----------------+
```

### How to collect the list of all prices from each category?
We can use `collect_list()` function to list out all the prices from each category.
`collect_set()` will list out the prices of all unique items from each category.
```scala
val result = priceAfterTax.groupBy("Category")
  .agg(collect_list("MRP").alias("All sales"),
    collect_set("Price After Tax").alias("Billable Amount"))
result.show()
```
**Output**
```text
+----------+-------------+--------------------+
|  Category|    All sales|     Billable Amount|
+----------+-------------+--------------------+
|Stationery|[23, 50, 120]|[54.0, 129.6, 24.84]|
|   Apparel|   [455, 901]|[491.4, 973.07999...|
|     Dairy|     [57, 50]|       [54.0, 61.56]|
|   Clothes|   [799, 570]|     [615.6, 862.92]|
+----------+-------------+--------------------+
```

### Is it possible to combine aggregate functions with list operations?
Yes, we can combine our usual aggregate functions. Let us find the total of each category of product purchased.
```scala
val subTotal = priceAfterTax.groupBy("Category")
  .agg(collect_list("Price After Tax").alias("Billable Amount"),
    sum("Price After Tax").alias("Sub Total"))

subTotal.show()
```
**Output**
```text
+----------+--------------------+---------+
|  Category|     Billable Amount|Sub Total|
+----------+--------------------+---------+
|Stationery|[24.84, 54.0, 129.6]|   208.44|
|   Apparel|[491.4, 973.07999...|  1464.48|
|     Dairy|       [61.56, 54.0]|   115.56|
|   Clothes|     [862.92, 615.6]|  1478.52|
+----------+--------------------+---------+
```

Similarly, we can even apply other aggregate functions to get more insights.
We can combine `count()`, `min()`, `max()` as well.

Let us see one more example where we calculate average billing of each category.
```scala
val catAvg = priceAfterTax.groupBy("Category")
      .agg(collect_list("Price After Tax").alias("Billable Amount"),
        avg("Price After Tax").alias("Average Price "))

catAvg.show()
```
**Output**
```text
+----------+--------------------+--------------+
|  Category|     Billable Amount|Average Price |
+----------+--------------------+--------------+
|Stationery|[24.84, 54.0, 129.6]|         69.48|
|   Apparel|[491.4, 973.07999...|        732.24|
|     Dairy|       [61.56, 54.0]|         57.78|
|   Clothes|     [862.92, 615.6]|        739.26|
+----------+--------------------+--------------+
```

### How to Map individual elements to their values while grouping them as a list?
It is easy to map individual elements to their respective values while grouping them. This function is very helpful when we want to know what value a particular record is taking.
In the above dataframe, let us try to see how it is done.

We can do it using `map_from_entries()` function which maps each item to its corresponding value.
```scala
val mapElements = priceAfterTax.groupBy("Category")
      .agg(collect_list("Price After Tax").alias("Billable Amount"),
        map_from_entries(collect_list(struct(col("Item Name"), col("Price After Tax"))))
          .alias("Item-Price")
      )

mapElements.show(false)
```
**Output**
```text
+----------+--------------------------+------------------------------------------------------+
|Category  |Billable Amount           |Item-Price                                            |
+----------+--------------------------+------------------------------------------------------+
|Stationery|[24.84, 54.0, 129.6]      |[Paper Clips -> 24.84, Stapler -> 54.0, Pens -> 129.6]|
|Apparel   |[491.4, 973.0799999999999]|[Bag -> 491.4, Shoes -> 973.0799999999999]            |
|Dairy     |[61.56, 54.0]             |[Butter -> 61.56, Butter Milk -> 54.0]                |
|Clothes   |[862.92, 615.6]           |[Jeans -> 862.92, Shirt -> 615.6]                     |
+----------+--------------------------+------------------------------------------------------+
```

### Summary
In this article, we have seen:
- What List aggregation methods are .
- How they are useful when we are dealing with large amounts of data.
- How we can combine aggregate operations with list aggregation methods.
- How we can create maps of records using `map_from_entries()`.

### Related Articles
- [Grouping Operations](@/docs/spark/grouping-operations.md)

### References
- [groupby and aggregate list of values to one list based on index](https://stackoverflow.com/questions/49843559/spark-scala-groupby-and-aggregate-list-of-values-to-one-list-based-on-index)
- [GroupBy clause](https://spark.apache.org/docs/3.5.3/sql-ref-syntax-qry-select-groupby.html)
- [Aggregating all Column values within a Map after groupBy in Apache Spark](https://stackoverflow.com/questions/57787239/aggregating-all-column-values-within-a-map-after-groupby-in-apache-spark)