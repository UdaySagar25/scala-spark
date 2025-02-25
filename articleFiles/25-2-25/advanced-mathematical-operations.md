## Advanced Mathematical Operations

Continuing our discussion from the previous article, let us now look into few advanced mathematical operations on a dataframe.
We shall be discussing:
- `abs()`
- `ceil()`
- `exp()`
- `pow()`
- `sqrt()`

Assume that you are in a locality where there are 5 parks/play areas and their data is with you.
```text
+-------+--------+---------+---------+-----+
|Park ID|Distance|Perimeter|Elevation| Area|
+-------+--------+---------+---------+-----+
|      1|    1250|    150.7|      4.5|5.972|
|      2|     670|     78.3|     -3.2|0.642|
|      3|    1580|    225.1|      6.0| 86.8|
|      4|     340|    108.9|     -1.5|0.013|
|      5|    1100|     45.6|      5.5|1.898|
+-------+--------+---------+---------+-----+
```
Let us apply the above mathematical operations on this dataframe?

### How can we find absolute values in a dataframe?
Say, we want to find the absolute values of the elevation of the park. To find that, we can use `abs()` operator.
```scala
val absElevation=data.withColumn("Absolute Elevation", abs(col("Elevation")))
absElevation.show()
```

**Output**
```text
+-------+--------+---------+---------+-----+------------------+
|Park ID|Distance|Perimeter|Elevation| Area|Absolute Elevation|
+-------+--------+---------+---------+-----+------------------+
|      1|    1250|    150.7|      4.5|5.972|               4.5|
|      2|     670|     78.3|     -3.2|0.642|               3.2|
|      3|    1580|    225.1|      6.0| 8.68|               6.0|
|      4|     340|    108.9|     -1.5|0.013|               1.5|
|      5|    1100|     45.6|      5.5|1.898|               5.5|
+-------+--------+---------+---------+-----+------------------+
```

### How can we find the ceiling values in dataframe?
Ceiling values are values that are approximated to the next integer. Assume we have to find the ciel values of the Area of Park. To do this, we use `ceil()` operator.
```scala
val ceilArea=data.withColumn("Ceiling Area value", ceil(col("Area")))
ceilArea.show()
```
**Outupt**
```text
+-------+--------+---------+---------+-----+------------------+
|Park ID|Distance|Perimeter|Elevation| Area|Ceiling Area value|
+-------+--------+---------+---------+-----+------------------+
|      1|    1250|    150.7|      4.5|5.972|                 6|
|      2|     670|     78.3|     -3.2|0.642|                 1|
|      3|    1580|    225.1|      6.0| 8.68|                 9|
|      4|     340|    108.9|     -1.5|0.013|                 1|
|      5|    1100|     45.6|      5.5|1.898|                 2|
+-------+--------+---------+---------+-----+------------------+
```

### How can we find floor value of an angle?
Floor values are the values that are approximated to the previous integer. 
Assume, we want to find the floor value of the Perimeter To do this, we can use `floor()` operator.
```scala
val floorValue=data.withColumn("Floor Value",floor(col("Perimeter")))
floorValue.show()
```
**Output**
```text
+-------+--------+---------+---------+-----+-----------+
|Park ID|Distance|Perimeter|Elevation| Area|Floor Value|
+-------+--------+---------+---------+-----+-----------+
|      1|    1250|    150.7|      4.5|5.972|        150|
|      2|     670|     78.3|     -3.2|0.642|         78|
|      3|    1580|    225.1|      6.0| 8.68|        225|
|      4|     340|    108.9|     -1.5|0.013|        108|
|      5|    1100|     45.6|      5.5|1.898|         45|
+-------+--------+---------+---------+-----+-----------+
```

### How can we find exponential value in a dataframe?
Exponential values are the values obtained when `exp` is raised to a given number. We will use `exp()` operator to find the exponent value.
```scala
val expValue=data.withColumn("Exponential Value", exp(col("Area")))
expValue.show()
```
**Output**
```text
+-------+--------+---------+---------+-----+------------------+
|Park ID|Distance|Perimeter|Elevation| Area| Exponential Value|
+-------+--------+---------+---------+-----+------------------+
|      1|    1250|    150.7|      4.5|5.972|392.28946562499834|
|      2|     670|     78.3|     -3.2|0.642|1.9002776365552259|
|      3|    1580|    225.1|      6.0| 8.68| 5884.046591336165|
|      4|     340|    108.9|     -1.5|0.013|1.0130848673598092|
|      5|    1100|     45.6|      5.5|1.898| 6.672536016273524|
+-------+--------+---------+---------+-----+------------------+
```

### How to find the power of the number?
The power of a number refers to the result obtained when that number is multiplied by itself a specified number of times, as determined by the exponent.
Let us find the perimeter of the parks when they are raised to 2. We will use `pow()` operator to find the values when perimeter is raised to 2.
```scala
val powValue=data.withColumn("Power Value",pow(col("Perimeter"),2))
powValue.show()
```
**Output**
```text
+-------+--------+---------+---------+-----+------------------+
|Park ID|Distance|Perimeter|Elevation| Area|       Power Value|
+-------+--------+---------+---------+-----+------------------+
|      1|    1250|    150.7|      4.5|5.972|22710.489999999998|
|      2|     670|     78.3|     -3.2|0.642| 6130.889999999999|
|      3|    1580|    225.1|      6.0| 8.68|50670.009999999995|
|      4|     340|    108.9|     -1.5|0.013|11859.210000000001|
|      5|    1100|     45.6|      5.5|1.898|           2079.36|
+-------+--------+---------+---------+-----+------------------+
```

### How to find the square root of the values in a dataframe?
To find the square root of a given value, we can use the `sqrt()` operator.
```scala
val sqrtValue=data.withColumn("Square Root", sqrt(col("Distance")))
sqrtValue.show()
```
**Output**
```text
+-------+--------+---------+---------+-----+------------------+
|Park ID|Distance|Perimeter|Elevation| Area|       Square Root|
+-------+--------+---------+---------+-----+------------------+
|      1|    1250|    150.7|      4.5|5.972| 35.35533905932738|
|      2|     670|     78.3|     -3.2|0.642| 25.88435821108957|
|      3|    1580|    225.1|      6.0| 8.68| 39.74921382870358|
|      4|     340|    108.9|     -1.5|0.013|18.439088914585774|
|      5|    1100|     45.6|      5.5|1.898|   33.166247903554|
+-------+--------+---------+---------+-----+------------------+
```

### Summary
In this article, we have seen:
- Advanced mathematical operations
- How these mathematical operation can be applied in real time.

### Related Articles
- [Dataframe](@/docs/spark/dataframe.md)
- [Spark Data Types](@/docs/spark/datatypes.md)

### References
- [Spark Math Operations](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#math-functions)
- [Spark withColumn() performing power functions](https://stackoverflow.com/questions/33271558/spark-withcolumn-performing-power-functions)