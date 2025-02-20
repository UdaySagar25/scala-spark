## DATA TYPES

created on: 10-2-2025

**Apache Spark** gives the users the flexibility of handling data of different types seamlessly. Spark Data Types are usually categorized into 5 types
  
Data types are typically divided into 5 types 
1. Numeric Type
2. String Type
3. Boolean Type
4. DateTime Type
5. Complex Type

Let us understand how to deal with multiple data types with the help of dataframes that we have learned earlier.
Let's create a new dataframe with columns having multiple datatypes

```scala
// Creating DataFrame directly
val df = Seq(
  (1.toShort, "Ajay", 'A'.toString, 10L, "2010-01-01", 55.5F, 92.75, true, Seq("Singing", "Sudoku")),
  (2.toShort, "Bharghav", 'B'.toString, 20L, "2009-06-04", 63.2F, 88.5, false, Seq("Dancing", "Painting")),
  (3.toShort, "Chaitra", 'C'.toString, 30L, "2010-12-12", 60.1F, 75.8, true, Seq("Chess", "Long Walks")),
  (4.toShort, "Kamal", 'D'.toString, 40L, "2010-08-25", 75.0F, 82.3, false, Seq("Reading Books", "Cooking")),
  (5.toShort, "Sohaib", 'E'.toString, 50L, "2009-04-14", 70.8F, 90.6, true, Seq("Singing", "Cooking"))
).toDF("ID", "Name", "Grade", "LongValue", "DOB", "FloatMarks", "DoubleMarks", "IsActive", "Hobbies")

// Convert DOB column from String to DateType
val dfWithDate = df.withColumn("DOB", to_date($"DOB", "yyyy-MM-dd"))

dfWithDate.show(false)
dfWithDate.printSchema()

```
**Output**
```text
+---+--------+-----+---------+----------+----------+-----------+--------+------------------------+
|ID |Name    |Grade|LongValue|DOB       |FloatMarks|DoubleMarks|IsActive|Hobbies                 |
+---+--------+-----+---------+----------+----------+-----------+--------+------------------------+
|1  |Ajay    |A    |10       |2010-01-01|55.5      |92.75      |true    |[Singing, Sudoku]       |
|2  |Bharghav|B    |20       |2009-06-04|63.2      |88.5       |false   |[Dancing, Painting]     |
|3  |Chaitra |C    |30       |2010-12-12|60.1      |75.8       |true    |[Chess, Long Walks]     |
|4  |Kamal   |D    |40       |2010-08-25|75.0      |82.3       |false   |[Reading Books, Cooking]|
|5  |Sohaib  |E    |50       |2009-04-14|70.8      |90.6       |true    |[Singing, Cooking]      |
+---+--------+-----+---------+----------+----------+-----------+--------+------------------------+

Schema of the dataframe

root
 |-- ID: short (nullable = false)
 |-- Name: string (nullable = true)
 |-- Grade: string (nullable = true)
 |-- LongValue: long (nullable = false)
 |-- DOB: date (nullable = true)
 |-- FloatMarks: float (nullable = false)
 |-- DoubleMarks: double (nullable = false)
 |-- IsActive: boolean (nullable = false)
 |-- Hobbies: array (nullable = true)
 |    |-- element: string (containsNull = true)
```

### Numeric Type

##### How to define Byte type?
Byte types represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
Here we use `.toByte()` to convert the numeric values to ByteType
```scala
val schema = StructType(Seq(StructField("byte_value", ByteType, nullable = false)))
// Create DataFrame with ByteType
val data = Seq(Row(10.toByte), Row(-100.toByte), Row(127.toByte))
val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
```
**Output**
```text
+----------+
|byte_value|
+----------+
|        10|
|      -100|
|       127|
+----------+
```

#### How to define ShortType data?
The range of numbers from -32768 to 32767 are categorized as ShortType. These numbers are 2-bit signed integers.
Here we use `.toShort()` to define what type of data is to be stored in the dataframe.
In the above created dataframe, 
```scala 
1.toShort 
2.toShort
3.toShort
```
are explicitly defined to be stored as ShortType 

#### How to define LongType data?
Long type numbers are 8-byte signed integers, and the numbers range from -9223372036854775808 to 9223372036854775807.
Long type number are suffixed with `L` or `.toLong`
In the above dataframe, we have used `L` to define the long type variables
```scala
10L
20L
30L
40L
50L
```

#### How to define FloatType data?
Float type represents 4-byte floating point numbers
Floating numbers are suffixed with `F` or `.toFloat`.
In the above dataframe, we have used `F` to define the Floating type variables
```scala
55.5F
63.2F
60.1F
75.0F
70.8F
```

#### How to define DoubleType data?
Double type variables represents 8-byte floating point numbers.
Numbers are suffixed with either `.toDouble` or nothing.
In the above dataframe, The column **Double Marks** represents the double values

```scala
92.75
88.5
75.8
82.3
90.6
```

#### How to define StringType data?
String type data is a combination of character type data, put together. It is usually defined with `"String.."`. 
If we want to explicitly define a character as string, we use the method `.toString`
In the above dataframe, `.toString` method has been used to define the section as String Type.
```scala
'A'.toString
'B'.toString
'C'.toString
'D'.toString
'E'.toString
```

#### How to define BooleanType data?
Boolean variables take values as either **True** or **False**
In the above DataFrame, the column IsActive represents Boolean values.
```scala
true
false
true
false
true
```
#### How to define ArrayType data?
Array type variables represent a collection (or list) of elements of the same data type. 
In Spark, ArrayType is commonly used to store lists or multiple values in a single column.

In the above DataFrame, the column Hobbies is an ArrayType that contains multiple hobbies for each person.
```scala
Seq("Singing", "Sudoku")
Seq("Dancing", "Painting")
Seq("Chess", "Long Walks")
Seq("Reading Books", "Cooking")
Seq("Singing", "Cooking")
```

#### How to define DateType data?
Date type variables represent date values in the format yyyy-MM-dd.
These values can be used for date-based operations.
In the above DataFrame, the column DOB initially contains String values and is later converted to DateType using to_date().
```scala
"2010-01-01"
"2009-06-04"
"2010-12-12"
"2010-08-25"
"2009-04-14"
```

### Summary
Apache Spark provides flexibility in handling various data types, categorized into Numeric Types, String Types, Boolean Type, DateTime Type, and Complex Types.
We have seen how to use these datatypes in the dataframes.
What we have covered ?
- ByteType
- ShortType
- LongType
- FloatType
- DoubleType
- StringType
- BooleanType
- ComplexType ( Arrays )

### Related Articles
- [Understanding Dataframes](dataframe.md)
- [Dataframe Column Operations](df-column.md)

## References:
- [Spark Data Types](https://spark.apache.org/docs/3.5.3/sql-ref-datatypes.html)
- [Working with Dates and Time in Spark](https://www.databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html)
