## Select() v/s SelectExpr()

created on: 12-2-2025

Both `select()` and `selectExpr()` are used to display specific columns of the spark dataframe. 
But there are a couple of differences between them. In this article, let us discuss what are the differences.

For easy understanding purpose, we'll be using the previously created student dataframe. [Refer this article for the dataframe](mathOps.md)
```text
+----+--------+-----------+-----------+------------+
|Roll|    Name|Final Marks|Float Marks|Double Marks|
+----+--------+-----------+-----------+------------+
|   1|    Ajay|        300|       55.5|       92.75|
|   2|Bharghav|        350|       63.2|        88.5|
|   3| Chaitra|        320|       60.1|        75.8|
|   4|   Kamal|        360|       75.0|        82.3|
|   5|  Sohaib|        450|       70.8|        90.6|
+----+--------+-----------+-----------+------------+
```
### How is select() method different from selectExpr() in selecting columns of a dataframe?
Both `select()` and `selectExpr()` do the same job in selecting the columns. But they both differ in the way select command is written. Let's see how they differ.
```scala
df.select($"Roll", $"Name", $"Final Marks").show()

df.selectExpr("Roll", "Name", "`Final Marks`").show()
```
**Output**
```text
// Both the methods return the same dataframe in the output
+----+--------+-----------+
|Roll|    Name|Final Marks|
+----+--------+-----------+
|   1|    Ajay|        300|
|   2|Bharghav|        350|
|   3| Chaitra|        320|
|   4|   Kamal|        360|
|   5|  Sohaib|        450|
+----+--------+-----------+
```

### How does select() and selectExpr() differ in syntax while performing arithmetic operations?
While both `select()` and `selectExpr()` give the same output, the syntax for `selectExpr()` is easy to write with less complexity when compared to `select()`.
```scala
df.select($"Name",  ($"Float Marks" + 10).as("Updated Float Marks")).show()

df.selectExpr("Name","`Float Marks` + 10 AS `Updated Float Marks`").show()
```
**Output**
```text
// the output remains the same in both the cases
+--------+-------------------+
|    Name|Updated Float Marks|
+--------+-------------------+
|    Ajay|               65.5|
|Bharghav|               73.2|
| Chaitra|               70.1|
|   Kamal|               85.0|
|  Sohaib|               80.8|
+--------+-------------------+
```

### Summary
- In this article we have covered, key differences of `select()` and `selectExpr()` methods.
- We have also seen that `selectExpr()` syntax is eaiser, and it get rids of `.as()` method and replace it with `AS`.

### References
- [Spark Dataframe selectExpr()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.selectExpr.html)
- [Spark Dataframe select()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html)
