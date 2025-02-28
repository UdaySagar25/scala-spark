## Advanced User Defined Functions

In the previous article, we have seen basics of User Defined Function(UDFs), how to define UDF, different ways to define a UDF. 
Now we will discuss complex UDFs, handling null values using UDFs, etc.

We will be using the shopping bill dataframe that we used in the previous article.

```text
+--------+-----------+----------+---+------------------+-----------------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|
+--------+-----------+----------+---+------------------+-----------------+
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|
|       2|     Butter|     Dairy| 57|    51.30000000004|            61.56|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|
|       6|        Bag|   Apparel|455|             409.5|            491.4|
|       7|      Shoes|   Apparel|901|             810.9|    973.079999999|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|
|       9|       Pens|Stationery|120|             108.0|            129.6|
+--------+-----------+----------+---+------------------+-----------------+
```

### How to apply different value to each record in a dataframe?
Our aim is to apply custom discount to each category of item and that too only if the price is over the minimum required price.
This is usually either not possible or difficult with spark pre-defined functions. Thus, we will be using UDF here.
```scala
val finalDiscount = udf((category: String, discountedPrice: Double, priceAfterTax: Double) => {
  category match {
    case "Stationery" if priceAfterTax < 50 => discountedPrice * 0.95
    case "Clothes" if priceAfterTax > 800   => discountedPrice * 0.92
    case "Dairy" if discountedPrice < 50    => discountedPrice * 0.98
    case _                                  => discountedPrice
  }
})
    
val finalDiscountBill = priceAfterTax.withColumn("Final Discounted Price",
  finalDiscount(col("Category"), col("Discounted Price"), col("Price After Tax")))

finalDiscountBill.show()
```
**Output**
```text
+--------+-----------+----------+---+------------------+-----------------+----------------------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|Final Discounted Price|
+--------+-----------+----------+---+------------------+-----------------+----------------------+
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|                19.665|
|       2|     Butter|     Dairy| 57|    51.30000000004|            61.56|        51.30000000004|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|               661.572|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|                 513.0|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|                  44.1|
|       6|        Bag|   Apparel|455|             409.5|            491.4|                 409.5|
|       7|      Shoes|   Apparel|901|             810.9|   973.0799999999|                 810.9|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|                  45.0|
|       9|       Pens|Stationery|120|             108.0|            129.6|                 108.0|
+--------+-----------+----------+---+------------------+-----------------+----------------------+
```

### How can I handle null values with the help of UDF
Consider the student dataframe
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   85|
|   2|Bharghav|   76|
|   3| Chaitra| null|
|   4|    null|   90|
|   5|  Sohaib| null|
+----+--------+-----+
```
Let us try to replace null values using UDFs.
```scala
val replaceNullName = udf((name: String) => {
      Option(name).getOrElse("Unknown")
})

val replaceNullMarks = udf((marks: Integer) => {
  Option(marks).getOrElse(40)
})

val resultDF = studentData
  .withColumn("Name", replaceNullName(col("Name")))
  .withColumn("Marks", replaceNullMarks(col("Marks")))

resultDF.show()
```
Here, we are defining two UDFs, one will replace null values in Name column and the other will replace null values in Marks Column.

**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   85|
|   2|Bharghav|   76|
|   3| Chaitra|   40|
|   4| Unknown|   90|
|   5|  Sohaib|   40|
+----+--------+-----+
```

**Note:** For replacing null values, it is advised to use spark's predefined functions like `lit()` and `fill()`, as they are better optimized that UDFs.
UDFs might work with small frames, but it might not be the same with big data.

### How to define UDFs with multiple input parameters?
UDFs are capable to accept multiple column values as inputs and can perform complex operations as well.
Let us demonstrate this on the shopping bill dataframe.
```scala
val priceRemarkUDF = udf((category: String, mrp: Double, priceAfterTax: Double) => {
  category match {
    case "Stationery" if priceAfterTax < mrp * 1.2 => "Fairly Priced"
    case "Stationery" => "Overpriced"
    case "Dairy" if priceAfterTax > mrp * 1.1 => "Overpriced"
    case "Dairy" => "Fairly Priced"
    case "Clothes" if priceAfterTax > 800 => "Expensive"
    case "Clothes" if priceAfterTax < 500 => "Cheap"
    case "Clothes" => "Moderate"
    case "Apparel" if priceAfterTax > 900 => "Overpriced"
    case "Apparel" => "Reasonable"
    case _ => "Unknown"
  }
})

val dfWithRemark = priceAfterTax.withColumn("Final Remark", priceRemarkUDF(col("Category"), col("MRP"), col("Price After Tax")))
dfWithRemark.show(truncate = false)
```
**Output**
```text
+--------+-----------+----------+---+------------------+-----------------+-------------+
|Item no.|Item Name  |Category  |MRP|Discounted Price  |Price After Tax  |Final Remark |
+--------+-----------+----------+---+------------------+-----------------+-------------+
|1       |Paper Clips|Stationery|23 |20.7              |24.84            |Fairly Priced|
|2       |Butter     |Dairy     |57 |51.300000000000004|61.56            |Fairly Priced|
|3       |Jeans      |Clothes   |799|719.1             |862.92           |Expensive    |
|4       |Shirt      |Clothes   |570|513.0             |615.6            |Moderate     |
|5       |Butter Milk|Dairy     |50 |45.0              |54.0             |Fairly Priced|
|6       |Bag        |Apparel   |455|409.5             |491.4            |Reasonable   |
|7       |Shoes      |Apparel   |901|810.9             |973.0799999999999|Overpriced   |
|8       |Stapler    |Stationery|50 |45.0              |54.0             |Fairly Priced|
|9       |Pens       |Stationery|120|108.0             |129.6            |Fairly Priced|
+--------+-----------+----------+---+------------------+-----------------+-------------+
```

### Summary
In this article, we have seen
- Complex UDF for a dataframe.
- How we can handle dataframe with null value records using UDF.
- How to define and implement a UDF with multiple column inputs.

### Related Articles
- [User Defined Functions](@/docs/spark/user-defined-functions.md)
- [SparkSQL: How to deal with null values in user defined function?](https://stackoverflow.com/questions/32357164/sparksql-how-to-deal-with-null-values-in-user-defined-function)

### References 
- [Spark UDF error - Schema for type Any is not supported](https://stackoverflow.com/questions/37392900/spark-udf-error-schema-for-type-any-is-not-supported)
- [Pass multiple columns in UDF](https://stackoverflow.com/questions/42540169/pyspark-pass-multiple-columns-in-udf)
- [Spark UDF documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html)

















**UDF-2**
- Advanced usage of UDFs
- handling null values using UDFs
- multi column UDFs
- map types in UDFs
- testing UDFs