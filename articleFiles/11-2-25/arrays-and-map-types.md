## Arrays and Map Types

created on: 11-2-2025

updated on: 17-2-2025

### What are Arrays?
Arrays are a collection of elements within a single row. They are useful for storing multiple values corresponding to a single entity.

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

**Note:** Array indexing starts from **0** and for any given array of size n, the index of the last element would be **n-1**

### How to access elements of the array from a dataframe?
To access the elements of the array, we use `.withColumn()` method, inside which we specify which column's data to be processed.

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
To get the size/length of the array, `size()` method is used.
```scala
val arrSize=df.withColumn("Marks Length",size($"Marks"))
arrSize.show()
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
Spark has a method `array_contains()` which will return whether the array contains the value or not.
```scala
// returns true if the array contains marks '63'
val hasMarks=df.withColumn("Has Marks", array_contains($"Marks", 63))
hasMarks.show()
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

### How to explode the array elements?
Exploding the array elements means to create a new row/record for each of the array element. To accomplish this, we use the method
`explode(ColName)`, inside `withColumn()` method.
```scala
val arrExplode=df.withColumn("Marks",explode($"Marks"))
arrExplode.show()
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

### What are Maps?
Maps are a data type which store information in Key-Value pair format. 
They are similar to dictionaries and hash maps, which are widely used in other languages.

### How to create a dataframe with Map Type?
To create a dataframe with Map type rows, we use `Map()` method.
```scala
val mapDf=Seq(
      (1, Map("name"->"Ajay", "Marks"->"55")),
      (2, Map("name"->"Bharghav", "Marks"->"63")),
      (3, Map("name"->"Chaitra", "Marks"->"60")),
      (4, Map("name"->"Kamal", "Marks"->"75")),
      (5, Map("name"->"Sohaib", "Marks"->"70"))
    ).toDF("Id", "Details")

mapDf.show(truncate=false)
```
**Output**
```text
+---+-------------------------------+
|Id |Details                        |
+---+-------------------------------+
|1  |{name -> Ajay, Marks -> 55}    |
|2  |{name -> Bharghav, Marks -> 63}|
|3  |{name -> Chaitra, Marks -> 60} |
|4  |{name -> Kamal, Marks -> 75}   |
|5  |{name -> Sohaib, Marks -> 70}  |
+---+-------------------------------+
```

### How to access values from the Map in a dataframe?
```scala
val mapValues=mapDf.withColumn("Name", $"Details"("name")).withColumn("Marks", $"Details"("Marks"))
mapValues.show(truncate=false)
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

### How to separate keys and values of a Map in a dataframe?
To display the keys and their respective values separately in a dataframe, we use the methods `map_keys(ColName)` and `map_values(ColNames)`
```scala
val keyValue=mapDf.withColumn("Keys", map_keys($"Details")).withColumn("Values", map_values($"Details"))
keyValue.show(truncate=false)
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
- We now know how to store multiple elements under a single entity.This is done by Array Type.
- How to handle data which is of Array Type.
- We also got to know what dataframe array operations can be done and how do they help us.

### Related Articles.
- [Understanding dataframes](@/docs/spark/dataframe.md)
- [Spark data types](@/docs/spark/datatypes.md)

### References
- [Spark Arrays](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array.html)
- [Array Operations](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_contains.html)
- [Dataframe Maps](https://stackoverflow.com/questions/67083543/pyspark-sql-dataframe-map-with-multiple-data-types)


