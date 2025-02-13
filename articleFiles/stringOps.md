## String Operations

created on: 12-2-2025

Let us now dive into understanding string operations that can be done on a Spark Dataframe.
String operations on Dataframes are categorized into 3 types
- Basic String functions
- Substring operations
- String concatenation

Before we begin, let us create a new dataframe 
```scala
val df: DataFrame=Seq(
      (1, "Ajay", "Physics","Hyderabad"),
      (2, "Bharghav","Cyber Security","Mumbai"),
      (3, "Chaitra","Material Science", "Indore"),
      (4, "Kamal","Design", "Puri"),
      (5, "Sohaib","Nuclear Science", "Cochin")
    ).toDF("Roll", "Name","Dept", "Location")
```
**Output**
```text
+----+--------+----------------+---------+
|Roll|    Name|            Dept| Location|
+----+--------+----------------+---------+
|   1|    Ajay|         Physics|Hyderabad|
|   2|Bharghav|  Cyber Security|   Mumbai|
|   3| Chaitra|Material Science|   Indore|
|   4|   Kamal|          Design|     Puri|
|   5|  Sohaib| Nuclear Science|   Cochin|
+----+--------+----------------+---------+

We shall now see implementation of various string operations on the above dataframe.

### How to convert the string data to lowercase?
To convert the String data to lowercase, we use `lower(col())` inside the `.withColumn()` method
```scala
val lowerDf=df.withColumn("Lower Case", lower(col("Name")))
lowerDf.show()
```
**Output**
```text
+----+--------+----------------+---------+----------+
|Roll|    Name|            Dept| Location|Lower Case|
+----+--------+----------------+---------+----------+
|   1|    Ajay|         Physics|Hyderabad|      ajay|
|   2|Bharghav|  Cyber Security|   Mumbai|  bharghav|
|   3| Chaitra|Material Science|   Indore|   chaitra|
|   4|   Kamal|          Design|     Puri|     kamal|
|   5|  Sohaib| Nuclear Science|   Cochin|    sohaib|
+----+--------+----------------+---------+----------+
```

### How to convert the string data to uppercase?
To convert the String data to lowercase, we use `upper(col())` inside the `.withColumn()` method
```scala
val upperDf=df.withColumn("upper Case", lower(col("Dept")))
upperDf.show()
```
**Output**
```text
+----+--------+----------------+---------+----------------+
|Roll|    Name|            Dept| Location|      Upper Case|
+----+--------+----------------+---------+----------------+
|   1|    Ajay|         Physics|Hyderabad|         PHYSICS|
|   2|Bharghav|  Cyber Security|   Mumbai|  CYBER SECURITY|
|   3| Chaitra|Material Science|   Indore|MATERIAL SCIENCE|
|   4|   Kamal|          Design|     Puri|          DESIGN|
|   5|  Sohaib| Nuclear Science|   Cochin| NUCLEAR SCIENCE|
+----+--------+----------------+---------+----------------+
```

### How to trim white spaces in the dataframe?
We use `trim(col())` method inside `.withColumn()` method. This method trims the white spaces from both the sides of the colum value
```scala
val dfTrim = df.withColumn("Loc Trimmed", trim(col("Location")))
dfTrim.show()
```
**Output**
```text
+----+--------+----------------+-------------+-----------+
|Roll|    Name|            Dept|     Location|Loc Trimmed|
+----+--------+----------------+-------------+-----------+
|   1|    Ajay|         Physics|  Hyderabad  |  Hyderabad|
|   2|Bharghav|  Cyber Security|     Mumbai  |     Mumbai|
|   3| Chaitra|Material Science|      Indore |     Indore|
|   4|   Kamal|          Design|       Puri  |       Puri|
|   5|  Sohaib| Nuclear Science|      Cochin |     Cochin|
+----+--------+----------------+-------------+-----------+
```
### How to find the length of the string value in a dataframe?
To find the length of the sting column values, use the method `length(col())` inside the `.withColumn()` method
```scala
val dfLength = df.withColumn("Name Length", length(col("Name")))
dfLength.show()
```
**Output**
```text
+----+--------+----------------+---------+-----------+
|Roll|    Name|            Dept| Location|Name Length|
+----+--------+----------------+---------+-----------+
|   1|    Ajay|         Physics|Hyderabad|          4|
|   2|Bharghav|  Cyber Security|   Mumbai|          8|
|   3| Chaitra|Material Science|   Indore|          7|
|   4|   Kamal|          Design|     Puri|          5|
|   5|  Sohaib| Nuclear Science|   Cochin|          6|
+----+--------+----------------+---------+-----------+
```
### How to reverse the string variables in a dataframe?
To reverse the string values in a dataframe, we use `reverse(col())` inside the `.withColumn()` method
```scala
val dfReverse = df.withColumn("Dept Reverse", reverse(col("Dept")))
dfReverse.show()
```
**Output**
```text
+----+--------+----------------+---------+----------------+
|Roll|    Name|            Dept| Location|    Dept Reverse|
+----+--------+----------------+---------+----------------+
|   1|    Ajay|         Physics|Hyderabad|         scisyhP|
|   2|Bharghav|  Cyber Security|   Mumbai|  ytiruceS rebyC|
|   3| Chaitra|Material Science|   Indore|ecneicS lairetaM|
|   4|   Kamal|          Design|     Puri|          ngiseD|
|   5|  Sohaib| Nuclear Science|   Cochin| ecneicS raelcuN|
+----+--------+----------------+---------+----------------+
```

### How to extract a substring from the original string?
We use the method `substring(col())` inside `.withColumn()` method to get the substring of the specified column
```scala
// displays the first 3 characters of the values under "Name"
val dfSubstring = df.withColumn("Name Substring", substring(col("Name"), 1, 3))
dfSubstring.show()
```
**Output**
```text
+----+--------+----------------+---------+--------------+
|Roll|    Name|            Dept| Location|Name Substring|
+----+--------+----------------+---------+--------------+
|   1|    Ajay|         Physics|Hyderabad|           Aja|
|   2|Bharghav|  Cyber Security|   Mumbai|           Bha|
|   3| Chaitra|Material Science|   Indore|           Cha|
|   4|   Kamal|          Design|     Puri|           Kam|
|   5|  Sohaib| Nuclear Science|   Cochin|           Soh|
+----+--------+----------------+---------+--------------+
```
### How to concatenate two string values in a dataframe?
To concatenate two string columns, we use the method `concat(col("col-1"), lit(" "), col("col-2"))` inside `.withColumn()` method
```scala
val dfConcat = df.withColumn("Name-City", concat(col("Name"), lit(" - "), col("Location")))
dfConcat.show()
```
**Output**
```text
+----+--------+----------------+---------+-----------------+
|Roll|    Name|            Dept| Location|        Name-City|
+----+--------+----------------+---------+-----------------+
|   1|    Ajay|         Physics|Hyderabad| Ajay - Hyderabad|
|   2|Bharghav|  Cyber Security|   Mumbai|Bharghav - Mumbai|
|   3| Chaitra|Material Science|   Indore| Chaitra - Indore|
|   4|   Kamal|          Design|     Puri|     Kamal - Puri|
|   5|  Sohaib| Nuclear Science|   Cochin|  Sohaib - Cochin|
+----+--------+----------------+---------+-----------------+
```

### Summary
- In this article, we have covered the String operations in a spark dataframe. 
- We have covered basic functions string functions, substring methods and string concatenation.
- These methods help us clean the data which will further help us with proper analysis.

### References
- [String Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#string-functions)
