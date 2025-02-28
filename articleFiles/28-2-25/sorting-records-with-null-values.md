## Sorting records with null values

In the previous article, we have seen how sorting is done, and how efficient it is to read the data. What if we have null values in the data? Will spark read the null values and sort the data?
We might have multiple questions like these while sorting. When dealing with big data, we will encounter records with null values, no matter how many precautions we take.

In this article, we will answer all these questions with suitable solutions. Before diving in, I suggest you to revisit [Sort Functions](@/docs/spark/sort-functions.md) article just to refresh your memory on sorting functions.

Consider the student dataframe which has null values in it
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
### How to sort the records which have null values?
Spark provides us with certain functions which allows us to sort the records with null values, either by placing them in the first or in the last.

These functions are
- asc_nulls_first
- asc_nulls_last
- desc_nulls_first
- desc_nulls_last

`asc_nulls_first` arranges the records in ascending order and then places the null valued records on the top.
```scala
val sortNullValues=studentData.orderBy(col("Name").asc_nulls_first)
sortNullValues.show()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   4|    null|   90|
|   1|    Ajay|   85|
|   2|Bharghav|   76|
|   3| Chaitra| null|
|   5|  Sohaib| null|
+----+--------+-----+
```
Since the sorting is happening based on the student names, null values in other columns are ignored.
 
**Note:** By default, Spark arranges the values in ascending order.

`asc_nulls_last` arranges the records in ascending order while keeping the null valued records to the end.
```scala
val sortNullValues=studentData.orderBy(col("Name").asc_nulls_last)
sortNullValues.show()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   85|
|   2|Bharghav|   76|
|   3| Chaitra| null|
|   5|  Sohaib| null|
|   4|    null|   90|
+----+--------+-----+
```

`desc_nulls_first` arranges the records in descending order and places the null valued records on the top.
```scala
val desNullValues=studentData.orderBy(col("Marks").desc_nulls_first)
desNullValues.show()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   5|  Sohaib| null|
|   3| Chaitra| null|
|   4|    null|   90|
|   1|    Ajay|   85|
|   2|Bharghav|   76|
+----+--------+-----+
```
Here the sorting is happening in descending order based on marks and records with null value in Marks column are placed on top. 
The null value in Name column is treated as a String.

`desc_nulls_last` arranges the records in descending order and places the null valued records in the bottom.
```scala
val desNullValues=studentData.orderBy(col("Marks").desc_nulls_last)
desNullValues.show()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   4|    null|   90|
|   1|    Ajay|   85|
|   2|Bharghav|   76|
|   5|  Sohaib| null|
|   3| Chaitra| null|
+----+--------+-----+
```

### Summary
In this article, we have seen:
- How Spark handles sorting records having null values.
- Different methods to sort records with null values.

### Related Articles
- [Sort Functions](@/docs/spark/sort-functions.md)

### References
- [Changing Nulls Ordering in Spark SQL](https://stackoverflow.com/questions/39381678/changing-nulls-ordering-in-spark-sql)
- [Sort Function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#sort-functions)
- [DataFrame Sort Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sort.html)
