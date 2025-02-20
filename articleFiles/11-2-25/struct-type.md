## Struct Data type

created on:11-2-2025

Struct data types is a complex data type in apache spark. It is used to store groups of multiple columns as a single entity.
Typically, Struct Type is a collection of Struct Field elements, which has 
- A name
- A datatype
- A nullable flag

### How to create a dataframe using StructType and StructField
```scala
val studentSchema=StructType(Array(
      StructField("ID",IntegerType,nullable=false),
      StructField("Name",StringType,nullable=false),
      StructField("Marks",IntegerType,nullable=false),
      StructField("Hobbies",StructType(Array(
        StructField("hobby1", StringType, nullable = false),
        StructField("hobby2", StringType, nullable = false)
      )),nullable = true)
    ))

    val sampledata=Seq(
      Row(1, "Ajay",55, Row("Singing", "Sudoku")),
      Row(2, "Bhargav",63, Row("Dancing", "Painting")),
      Row(3, "Chaitra",60, Row("Chess", "Long Walks")),
      Row(4, "Kamal", 75, Row("Reading Books", "Cooking")),
      Row(5, "Sohaib", 70, Row("Singing", "Cooking"))
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(sampledata), studentSchema)

    df.show(truncate=false)
```
**Output**
```text
+---+-------+-----+------------------------+
| ID|   Name|Marks|                 Hobbies|
+---+-------+-----+------------------------+
|  1|   Ajay|   55|       {Singing, Sudoku}|
|  2|Bhargav|   63|     {Dancing, Painting}|
|  3|Chaitra|   60|     {Chess, Long Walks}|
|  4|  Kamal|   75|{Reading Books, Cooking}|
|  5| Sohaib|   70|      {Singing, Cooking}|
+---+-------+-----+------------------------+
```

### How to access the nested fields of the dataframe?
In our example, let us try to access hobbies of the students
```scala
df.select(col("Name"),col("Hobbies.hobby1")).show()
```
**Output**
```text
+-------+-------------+
|   Name|       hobby1|
+-------+-------------+
|   Ajay|      Singing|
|Bhargav|      Dancing|
|Chaitra|        Chess|
|  Kamal|Reading Books|
| Sohaib|      Singing|
+-------+-------------+
```

### How to rename the nested column names?
```scala
df.select(col("Name"), col("Hobbies.hobby1").alias("hobby")).show()
```
```text
+-------+-------------+
|   Name|        hobby|
+-------+-------------+
|   Ajay|      Singing|
|Bhargav|      Dancing|
|Chaitra|        Chess|
|  Kamal|Reading Books|
| Sohaib|      Singing|
+-------+-------------+
```

### How to flatten the Struct columns?
We can use `selectExpr()` to flatten out the nested columns of the dataframe
```scala
df.selectExpr("Name","Hobbies.hobby1 as hobby1","Hobbies.hobby2 as hobby2").show()
```
**Output**
```text
+-------+-------------+----------+
|   Name|       hobby1|    hobby2|
+-------+-------------+----------+
|   Ajay|      Singing|    Sudoku|
|Bhargav|      Dancing|  Painting|
|Chaitra|        Chess|Long Walks|
|  Kamal|Reading Books|   Cooking|
| Sohaib|      Singing|   Cooking|
+-------+-------------+----------+
```

### How to filter dataframe rows based on nested field values?
```scala
df.filter(col("Hobbies.hobby1") === "Singing").show(truncate=false)
```
**Output**
```text
+---+------+-----+------------------+
| ID|  Name|Marks|           Hobbies|
+---+------+-----+------------------+
|  1|  Ajay|   55| {Singing, Sudoku}|
|  5|Sohaib|   70|{Singing, Cooking}|
+---+------+-----+------------------+
```

### Summmary
- Struct Data Type in Apache Spark is a complex data type used to store multiple columns as a single entity. 
- It consists of StructField elements, each having a name, data type, and nullable flag.
- `StructType()` method is used to define the schema with nested fields.
- `Row()` objects stores the data.

We have also looked at:
- How to create a dataframe of struct type
- How to access rows based on nested fields
- How to rename the nested column names
- How to flatten the dataframe column values
- How to filter rows based on nested structType values

### References
- [Struct Type](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.StructType.html)
- [Struct](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.struct.html)

