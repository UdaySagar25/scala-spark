## Sort functions

When dealing with large data, the data is always jumbled and can be very hard to read when there are null values as well. 
Spark functions are used to arrange the data in a specific order. This helps in optimizing performance, faster processing. 

In this article, we shall be looking at sorting the dataframe records in different ways. Sorting of the records can be done using the following functions.
- orderBy()
- sort functions

Let us use the shopping bill dataframe that we used during our discussion on grouping functions. 
To read more about grouping functions, Refer these articles [Grouping Operations](@/docs/spark/grouping-operations.md) and [GroupBy List Aggregations](@/docs/spark/groupBy-list-aggregations.md)
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

### How to order the dataframe alphabetically?
Let us assume that we have been asked to order the list of items purchased alphabetically. Since this is a small snippet of data, it is easy to decide the order. 
But when we are dealing with large data, that is not possible. To solve this, we will be using `orderBy()` function available in Spark.
```scala
priceAfterTax.orderBy(col("Item Name")).show()
```
Executing the above spark code will return the list of items arranged alphabetically.

**Output**
```text
+--------+-----------+----------+---+------------------+-----------------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|
+--------+-----------+----------+---+------------------+-----------------+
|       6|        Bag|   Apparel|455|             409.5|            491.4|
|       2|     Butter|     Dairy| 57|     51.3000000004|            61.56|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|
|       9|       Pens|Stationery|120|             108.0|            129.6|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|
|       7|      Shoes|   Apparel|901|             810.9|    973.079999999|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|
+--------+-----------+----------+---+------------------+-----------------+
```

### How to arrange values alphabetically inside a sub category and arrange the sub category?
It is possible in Spark to sort the records in a dataframe with respect to sub category. To accomplish this, we have to give two column names as input in the `orderBy()` function.
```scala
val sortedItems=priceAfterTax.orderBy(col("Category"),col("Item Name"))
sortedItems.show()
```
**Output**
```text
+--------+-----------+----------+---+------------------+-----------------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|
+--------+-----------+----------+---+------------------+-----------------+
|       6|        Bag|   Apparel|455|             409.5|            491.4|
|       7|      Shoes|   Apparel|901|             810.9|    973.079999999|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|
|       2|     Butter|     Dairy| 57|     51.3000000004|            61.56|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|
|       9|       Pens|Stationery|120|             108.0|            129.6|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|
+--------+-----------+----------+---+------------------+-----------------+
```
We can see that in the above dataframe, Item names have been sorted alphabetically inside each category, and also the categories are sorted alphabetically.

**Note:** Make sure to correctly give the order of column names correctly in the `orderBy()` function, as Spark follows the given order. If the order is wrong, then the output might be wrong as well.

### Is there any other way to sort the records without using `orderBy()` function?  
Spark has another function `sort()` which is an alias function of `orderBy()`. They both work in the same way and return the same output.
```scala
val sortPrice=priceAfterTax.sort(col("Price After Tax").asc)
sortPrice.show()
```
**Output**
```text
+--------+-----------+----------+---+------------------+-----------------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|
+--------+-----------+----------+---+------------------+-----------------+
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|
|       2|     Butter|     Dairy| 57|     51.3000000004|            61.56|
|       9|       Pens|Stationery|120|             108.0|            129.6|
|       6|        Bag|   Apparel|455|             409.5|            491.4|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|
|       7|      Shoes|   Apparel|901|             810.9|    973.079999999|
+--------+-----------+----------+---+------------------+-----------------+
```
**Note:** By default, the sorting happens in ascending order, but it is a good practice to mention how the ordering should be done.

### How to arrange the records in descending order?
To arrange the records in descending order, set the sort to descending using `.desc`.
```scala
val sortPriceDesc=priceAfterTax.sort(col("Price After Tax").desc)
sortPriceDesc.show()
```
**Output**
```text
+--------+-----------+----------+---+------------------+-----------------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|
+--------+-----------+----------+---+------------------+-----------------+
|       7|      Shoes|   Apparel|901|             810.9|    973.079999999|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|
|       6|        Bag|   Apparel|455|             409.5|            491.4|
|       9|       Pens|Stationery|120|             108.0|            129.6|
|       2|     Butter|     Dairy| 57|     51.3000000004|            61.56|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|
+--------+-----------+----------+---+------------------+-----------------+
```

### How to order the categories in descending order and the category elements in ascending order?
This can be accomplished by using either `orderBy()` and `sort()` function where we define the columns and in what order they are to be sorted.
```scala
val multiSort=priceAfterTax.orderBy(col("Category").desc, col("Item Name").asc)
    multiSort.show()
```
**Output**
```text
+--------+-----------+----------+---+------------------+-----------------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|
+--------+-----------+----------+---+------------------+-----------------+
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|
|       9|       Pens|Stationery|120|             108.0|            129.6|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|
|       2|     Butter|     Dairy| 57|51.300000000000004|            61.56|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|
|       6|        Bag|   Apparel|455|             409.5|            491.4|
|       7|      Shoes|   Apparel|901|             810.9|973.0799999999999|
+--------+-----------+----------+---+------------------+-----------------+
```

### Summary
In this article, we have seen:
- What sorting is and why it is important.
- Different types of sorting methods.
- How we can sort records based on multiple columns.
- How we can sort records based on multiple columns where each column is sorted in a specific way.

### Related Articles
- [Select vs SelectExpr](@/docs/spark/select-vs-selectexpr.md)
- [Grouping Operations](@/docs/spark/grouping-operations.md)
- [GroupBy List Aggregations](@/docs/spark/groupBy-list-aggregations.md)

### References
- [Sort Function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#sort-functions)
- [How to sort by column in descending order in Spark SQL?](https://stackoverflow.com/questions/30332619/how-to-sort-by-column-in-descending-order-in-spark-sql)
- [DataFrame Sort Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sort.html)