## Arrays and Map Types

### What are arrays and map types?
Arrays are a collection of elements within a single row. They are useful for storing multiple values corresponding to a single entity.
Maps are a data type which store information in Key-Value pair format. They are similar to dictionaries and hash maps, which are widely used in other languages.

Let us start with creating a Dataframe of Array type
```scala
val df: DataFrame = Seq(
      (1, "Ajay", Array(55,65,72)),
      (2, "Bharghav", Array(63,79,71)),
      (3, "Chaitra", Array(60,63, 75)),
      (4, "Kamal", Array(73,75,69)),
      (5, "Sohaib", Array(86,74,70))
    ).toDF("Roll", "Name", "Marks")

df.show()
```
**Output**
```text
+----+--------+------------+
|Roll|    Name|       Marks|
+----+--------+------------+
|   1|    Ajay|[55, 65, 72]|
|   2|Bharghav|[63, 79, 71]|
|   3| Chaitra|[60, 63, 75]|
|   4|   Kamal|[73, 75, 69]|
|   5|  Sohaib|[86, 74, 70]|
+----+--------+------------+
```

### How to access elements of the array from a dataframe?
```scala
val arrayElements=df.withColumn("Marks",$"Marks"(1))
arrayElements.show()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   65|
|   2|Bharghav|   79|
|   3| Chaitra|   63|
|   4|   Kamal|   75|
|   5|  Sohaib|   74|
+----+--------+-----+
```

### How to check the length of the array in a dataframe?
To get the size/length of the array, `size()` method is used
```scala
df.withColumn("Marks Length",size($"Marks")).show()
```
**Output**
```text
+----+--------+------------+------------+
|Roll|    Name|       Marks|Marks Length|
+----+--------+------------+------------+
|   1|    Ajay|[55, 65, 72]|           3|
|   2|Bharghav|[63, 79, 71]|           3|
|   3| Chaitra|[60, 63, 75]|           3|
|   4|   Kamal|[73, 75, 69]|           3|
|   5|  Sohaib|[86, 74, 70]|           3|
+----+--------+------------+------------+
```

### How to check if an array contains a specific value or not?
To answer this, spark has a method `array_contains()` which will return whether the array contains the value or not
```scala
// returns true if the array contains marks '63'
df.withColumn("Has Marks", array_contains($"Marks", 63)).show()
```
**Output**
```text
+----+--------+------------+---------+
|Roll|    Name|       Marks|Has Marks|
+----+--------+------------+---------+
|   1|    Ajay|[55, 65, 72]|    false|
|   2|Bharghav|[63, 79, 71]|     true|
|   3| Chaitra|[60, 63, 75]|     true|
|   4|   Kamal|[73, 75, 69]|    false|
|   5|  Sohaib|[86, 74, 70]|    false|
+----+--------+------------+---------+
```
### How to explode array elements?
to explode an array elements, i.e., to create a new row for each of array elements, use `explode()` method.
```scala
df.withColumn("Marks",explode($"Marks")).show()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   55|
|   1|    Ajay|   65|
|   1|    Ajay|   72|
|   2|Bharghav|   63|
|   2|Bharghav|   79|
|   2|Bharghav|   71|
|   3| Chaitra|   60|
|   3| Chaitra|   63|
|   3| Chaitra|   75|
|   4|   Kamal|   73|
|   4|   Kamal|   75|
|   4|   Kamal|   69|
|   5|  Sohaib|   86|
|   5|  Sohaib|   74|
|   5|  Sohaib|   70|
+----+--------+-----+
```

### How to create a dataframe with Map Type?
To create a dataframe with Map type rows, we use `Map()` method
```scala
val mapDf=Seq(
      (1, Map("name"->"Ajay", "Marks"->55)),
      (2, Map("name"->"Bharghav", "Marks"->63)),
      (3, Map("name"->"Chaitra", "Marks"->60)),
      (4, Map("name"->"Kamal", "Marks"->75)),
      (5, Map("name"->"Sohaib", "Marks"->70))
    ).toDF("Id", "Details")
mapDf.show()
```
**Output**
```text
DataFrame:
+---+--------------------+
| Id|             Details|
+---+--------------------+
|  1|Map(name -> Ajay,...|
|  2|Map(name -> Bhargh..|
|  3|Map(name -> Chai....|
|  4|Map(name -> Kamal...|
|  5|Map(name -> Sohaib..|
+---+--------------------+
```

### How to access Map values of a dataframe?
```scala
mapDf.withColumn("Name", $"Details"("name"))
.withColumn("Marks", $"Details"("Marks"))
.show()
```
**Output**
```text
+---+----------------------------------+--------+-----+
| Id|                           Details|    Name|Marks| 
+---+----------------------------------+--------+-----+
|  1|Map(name -> Ajay, Marks -> 55)    |    Ajay|   55|
|  2|Map(name -> Bharghav, Marks -> 63)|Bharghav|   63|
|  3|Map(name -> Chaitra, Marks -> 60) | Chaitra|   60|
|  4|Map(name -> Kamal, Marks -> 75)   |   Kamal|   75| 
|  5|Map(name -> Sohaib, Marks -> 70)  |  Sohaib|   70|  
+---+----------------------------------+--------+-----+
```
### How to map key-value pairs in a dataframe?
```scala
mapDf.withColumn("Keys", map_keys($"Details"))
  .withColumn("Values", map_values($"Details"))
  .show()
 ```
**Output**
```text
+---+----------------------------------+-------------+-------------+
| Id|                           Details|         Keys|       Values|
+---+----------------------------------+-------------+-------------+
|  1|Map(name -> Ajay, Marks -> 55)    |[name, Marks]|   [Ajay, 55]|
|  2|Map(name -> Bharghav, Marks -> 63)|[name, Marks]|[Bharghav, 63|
|  3|Map(name -> Chaitra, Marks -> 60) |[name, Marks]|[Chaitra, 60]|
|  4|Map(name -> Kamal, Marks -> 75)   |[name, Marks]|  [Kamal, 75]|
|  5|Map(name -> Sohaib, Marks -> 70)  |[name, Marks]| [Sohaib, 70]|
+---+----------------------------------+-------------+-------------+
```

### Summary
- We now know how to store multiple elements under a single entity.
- This is done by Array Type. How to handle data which is of Array Type.
- We also got to know what dataframe array operations can be done and how do they help us.

### Related Articles.
- [Understanding dataframes](dataframe.md)
- [Spark data types](datatypes.md)

### References
- [Spark Arrays](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array.html)
- [Array Operations](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_contains.html)
- [Dataframe Maps](https://stackoverflow.com/questions/67083543/pyspark-sql-dataframe-map-with-multiple-data-types)


