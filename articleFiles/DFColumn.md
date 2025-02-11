## DataFrame Column Manipulations
Let us create a new data frame. On this, we will demonstrate the column manipulation operations
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
|Roll|    Name|Marks|[moreQuestions.md](moreQuestions.md)
+----+--------+-----+
|   1|    Ajay|   55|
|   2|Bharghav|   63|
|   3| Chaitra|   60|
|   4|   Kamal|   75|
|   5|  Sohaib|   70|
+----+--------+-----+
```
### 1. How to add new Columns to the dataframe ?
`.withColumn()` method is used to add a new column based on the existing column
```scala
// Adding a new column to the created dataframe
val add_column=df.withColumn("Updated Marks", col("Marks")+5)
add_column.show()
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

### 2. How to rename an existing column name ?
`.withColumnRenamed()` method is used to rename the column names
```scala
// Renaming an existing column
val rename_column= df.withColumnRenamed("Roll", "Roll Number")
rename_column.show()
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
### 3. How to drop a column from the dataframe ?
`.drop()` is used to drop the specific columns from the dataframe
```scala
// Dropping an existing column
val drop_column=df.drop("Roll")
drop_column.show()
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
val select_columns = df.select("Name", "Marks")
select_columns.show()
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
### 5. How to Filter Rows based on column values ?
`filter()` method is used to filter the rows, based on the column values
```scala
// Filtering Based on Column Values
val filter_column=add_column.filter(col("Updated Marks")>=65)
filter_column.show()
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
### 6. How to create a column with conditional values
`.when()` and `.otherwise()` methods are used to create columns whose values are based on the given conditions
```scala
// Creating columns with conditional values
val dfCategory=add_column.withColumn("Division", when(col("Updated Marks")>70, "Distinction").otherwise("First class"))
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




