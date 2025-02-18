## Dataframe Column Manipulations

created on: 10-2-2025

Dataframe column manipulations on spark dataframes, are operations that are done on dataframe columns which help in cleaning the data, feature engineering and transforming the data for better analysis.

Let us now look into what are the column operations that can be done on a spark dataframe.
Firstly, let's start by creating a dataframe. To know more about dataframe creation, [Refer this link](dataframe.md)

```scala
import spark.implicits._
val df: DataFrame=Seq(
  (1, "Ajay", 55),
  (2, "Bharghav", 63),
  (3, "Chaitra", 60),
  (4, "Kamal", 75),
  (5, "Sohaib", 70)
).toDF("Roll", "Name", "Marks")

df.show()
```

**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   55|
|   2|Bharghav|   63|
|   3| Chaitra|   60|
|   4|   Kamal|   75|
|   5|  Sohaib|   70|
+----+--------+-----+
```

### 1. How to add new Columns to the dataframe?
`.withColumn()` method is used to add a new column based on the existing column
```scala
// Adding a new column to the created dataframe
val addColumn=df.withColumn("Updated Marks", col("Marks")+5)
addColumn.show()
```
**Output**
```text
+----+--------+-----+-------------+
|Roll|    Name|Marks|Updated Marks|
+----+--------+-----+-------------+
|   1|    Ajay|   55|           60|
|   2|Bharghav|   63|           68|
|   3| Chaitra|   60|           65|
|   4|   Kamal|   75|           80|
|   5|  Sohaib|   70|           75|
+----+--------+-----+-------------+
```

### 2. How to rename an existing column name?
`.withColumnRenamed()` method is used to rename the column names
```scala
// Renaming an existing column
val renameColumn= df.withColumnRenamed("Roll", "Roll Number")
renameColumn.show()
```
**Output**
```text
+-----------+--------+-----+
|Roll Number|    Name|Marks|
+-----------+--------+-----+
|          1|    Ajay|   55|
|          2|Bharghav|   63|
|          3| Chaitra|   60|
|          4|   Kamal|   75|
|          5|  Sohaib|   70|
+-----------+--------+-----+
```

### 3. How to drop a column from the dataframe?
`.drop()` is used to drop the specific columns from the dataframe
```scala
// Dropping an existing column
val dropColumn=df.drop("Roll")
dropColumn.show()
```
**Output**
```text
+--------+-----+
|    Name|Marks|
+--------+-----+
|    Ajay|   55|
|Bharghav|   63|
| Chaitra|   60|
|   Kamal|   75|
|  Sohaib|   70|
+--------+-----+
```

### 4. How to select specific columns from the dataframe?
`.select()` method is used to select the desired columns to be displayed of the dataframe
```scala
val selectColumns = df.select("Name", "Marks")
selectColumns.show()
```
**Output**
```text
+--------+-----+
|    Name|Marks|
+--------+-----+
|    Ajay|   55|
|Bharghav|   63|
| Chaitra|   60|
|   Kamal|   75|
|  Sohaib|   70|
+--------+-----+
```

### 5. How to Filter Rows based on column values?
`filter()` method is used to filter the rows, based on the column values
```scala
// Filtering Based on Column Values
val filterColumn=addColumn.filter(col("Updated Marks")>=65)
filterColumn.show()
```
**Outupt**
```text
+----+--------+-----+-------------+
|Roll|    Name|Marks|Updated Marks|
+----+--------+-----+-------------+
|   2|Bharghav|   63|           68|
|   3| Chaitra|   60|           65|
|   4|   Kamal|   75|           80|
|   5|  Sohaib|   70|           75|
+----+--------+-----+-------------+
```

### 6. How to create a column with conditional values?
`.when()` and `.otherwise()` methods are used to create columns whose values are based on the given conditions
```scala
// Creating columns with conditional values
val dfCategory=addColumn.withColumn("Division", when(col("Updated Marks")>70, "Distinction").otherwise("First class"))
dfCategory.show()
```
```text
+----+--------+-----+-------------+-----------+
|Roll|    Name|Marks|Updated Marks|   Division|
+----+--------+-----+-------------+-----------+
|   1|    Ajay|   55|           60|First class|
|   2|Bharghav|   63|           68|First class|
|   3| Chaitra|   60|           65|First class|
|   4|   Kamal|   75|           80|Distinction|
|   5|  Sohaib|   70|           75|Distinction|
+----+--------+-----+-------------+-----------+
```

### Summary
This article covers the basics of column manipulations of a dataframe, like adding new columns, renaming the column names, filtering and dropping columns that are not required to us.
What have we covered?
- Adding new columns
- Rename the existing columns
- Drop a particular column from the dataframe
- Select and print only required columns of the dataframe
- How to filter rows based on the column values
- Creating a new column by applying conditions on existing columns

### Related Articles
- [Understanding dataframes](dataframe.md)

### References
- [Getting Started with dataframes](https://www.databricks.com/spark/getting-started-with-apache-spark/dataframes)
- [Spark Column Operations](https://spark.apache.org/docs/3.5.1/api/java/org/apache/spark/sql/Column.html)



