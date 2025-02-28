## User Defined Functions(UDF)

### What are User Defined Functions?
A UDF is a function you write yourself to perform operations on Spark data that aren’t covered by Spark’s standard functions. They’re useful when you need to apply complex transformations, custom computations, or domain-specific logic to your dataframe.
We UDFs in Spark when the required transformation is not available as built-in Spark function.

### How to create a simple User Defined Function(UDF)?
Let us see how to create a simple UDF, which will capitalize the second letter of a Name values .

Consider the student dataframe.
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   85|
|   2|Bharghav|   76|
|   3| Chaitra|   70|
|   4|   Kamal|   90|
|   5|  Sohaib|   83|
+----+--------+-----+
```
```scala
// create the user defined function outside main object
object CapitalizeSecondUDF {
  def capitalizeSecond(text: String): String = {
    if (text != null && text.nonEmpty) {
      text.substring(1, 2).toUpperCase + text.substring(2).toLowerCase
    } else {
      text
    }
  }
  val capitalizeUDF = udf(capitalizeSecond _)
}
```
```scala
// call the function with the parameter inside the main object
val result = df.withColumn("capitalized", CapitalizeSecondUDF.capitalizeUDF(col("Name")))
result.show()
```
**Output**
```text
+----+--------+-----+-----------+
|Roll|    Name|Marks|capitalized|
+----+--------+-----+-----------+
|   1|    Ajay|   85|       AJay|
|   2|Bharghav|   76|   BHarghav|
|   3| Chaitra|   70|    CHaitra|
|   4|   Kamal|   90|      KAmal|
|   5|  Sohaib|   83|     SOhaib|
+----+--------+-----+-----------+
```
We can see that the second letter of the Name has been capitalized using user defined functions. This operation is not possible with default spark functions.

Let us consider little complex dataframe on which we can perform more user defined options. We will use the shopping bill dataframe that we used in the previous articles.
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
In the above example, we defined a user functions as separate object, outside the main object and called it in the main function. 
Well, that is one way of defining UDF. There is another way as well. We can declare a variable as a UDF using `UserDefinedFunction` function. Let us see how we can do that.

### How to classify items based on their price?
```scala
import org.apache.spark.sql.expressions.UserDefinedFunction

val classifyPriceUDF: UserDefinedFunction = udf((price: Double) => {
  if (price < 100) "Cheap"
  else if (price >= 100 && price <= 500) "Moderate"
  else "Expensive"
})

val enrichedDF = priceAfterTax.withColumn("Price Category", classifyPriceUDF(col("Price After Tax")))
enrichedDF.show()
```
**Output**
```text
+--------+-----------+----------+---+------------------+-----------------+--------------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|Price Category|
+--------+-----------+----------+---+------------------+-----------------+--------------+
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|         Cheap|
|       2|     Butter|     Dairy| 57|    51.30000000004|            61.56|         Cheap|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|     Expensive|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|     Expensive|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|         Cheap|
|       6|        Bag|   Apparel|455|             409.5|            491.4|      Moderate|
|       7|      Shoes|   Apparel|901|             810.9|    973.079999999|     Expensive|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|         Cheap|
|       9|       Pens|Stationery|120|             108.0|            129.6|      Moderate|
+--------+-----------+----------+---+------------------+-----------------+--------------+
```

Let us see one more example of how UDF is implemented.

### How to add a Custom Tax Slab column based on category.
```scala
val taxSlabUDF = udf((category: String) => {
      category match {
        case "Stationery" => "5%"
        case "Dairy"      => "12%"
        case "Clothes"    => "18%"
        case "Apparel"    => "18%"
        case _            => "10%"
      }
    })

val dfWithTaxSlab = priceAfterTax.withColumn("Tax Slab", taxSlabUDF(col("Category")))
dfWithTaxSlab.show()
```
**Output**
```text
+--------+-----------+----------+---+------------------+-----------------+--------+
|Item no.|  Item Name|  Category|MRP|  Discounted Price|  Price After Tax|Tax Slab|
+--------+-----------+----------+---+------------------+-----------------+--------+
|       1|Paper Clips|Stationery| 23|              20.7|            24.84|      5%|
|       2|     Butter|     Dairy| 57|51.300000000000004|            61.56|     12%|
|       3|      Jeans|   Clothes|799|             719.1|           862.92|     18%|
|       4|      Shirt|   Clothes|570|             513.0|            615.6|     18%|
|       5|Butter Milk|     Dairy| 50|              45.0|             54.0|     12%|
|       6|        Bag|   Apparel|455|             409.5|            491.4|     18%|
|       7|      Shoes|   Apparel|901|             810.9|973.0799999999999|     18%|
|       8|    Stapler|Stationery| 50|              45.0|             54.0|      5%|
|       9|       Pens|Stationery|120|             108.0|            129.6|      5%|
+--------+-----------+----------+---+------------------+-----------------+--------+
```
In the user defined function, we are using pattern matching to check the value of the category and returns the corresponding tax rates.

We shall see more about User Defined Functions in the upcoming article.

### Summary
In this article, we have see:
- What User Defined Functions are and why they are used.
- How we can create a simple UDF.
- Different ways to create UDF.
- How we can perform String operations and numeric operations using UDF.
 
### Related Article
- [Mathematical Operations](@/docs/spark/mathematical-operations.md)
- [String Operations](@/docs/spark/string-operations.md)
- [Advanced Mathematical Operations](@/docs/spark/advanced-mathematical-operations.md)


### References
- [What are user-defined functions (UDFs)?](https://docs.databricks.com/aws/en/udf/)
- [Scalar User Defined Functions](https://spark.apache.org/docs/3.5.2/sql-ref-functions-udf-scalar.html)
- [Creating User Defined Function in Spark-SQL](https://stackoverflow.com/questions/25031129/creating-user-defined-function-in-spark-sql)


