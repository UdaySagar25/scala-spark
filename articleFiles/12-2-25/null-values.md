## Null Values

created on: 12-2-2025

In any form of data, existence of null values or missing values is inevitable. And sometimes, these null values are very crucial. So we have to handle the null values to avoid misinterpretations and ambiguity.
Spark dataframes provide multiple ways to handle these null values. Let us now look at the common approaches of handling these null values.

Assume that we have a dataframe with null values.
```text
+----+-------+-----------+-----------+------------+
|Roll|   Name|Final Marks|Float Marks|Double Marks|
+----+-------+-----------+-----------+------------+
|   1|   Ajay|        300|       null|       92.75|
|   2|   null|        350|       63.2|        88.5|
|null|Chaitra|        320|       60.1|        75.8|
|   4|  Kamal|       null|       75.0|        null|
|   5| Sohaib|        450|       null|        90.6|
+----+-------+-----------+-----------+------------+
```
As we can see, there are many null values present in the dataframe. Let us now look at different ways to handle null values


### How to filter the null values from a dataframe?
One way to filter null values is to use `na.drop()`. Now `na.drop()` has multiple arguments, which will determine, how to filter the rows with null values.

```scala
val allNull=df.na.drop("all") // this omits the rows where all the values are null
allNull.show()

val fewNull=df.na.drop("any")// this omits the rows which has atleast 1 null value
fewNull.show()
```
**Output**
```text
Since all the rows have at most 1 null value, the first command will print all the rows

+----+-------+-----------+-----------+------------+
|Roll|   Name|Final Marks|Float Marks|Double Marks|
+----+-------+-----------+-----------+------------+
|   1|   Ajay|        300|       null|       92.75|
|   2|   null|        350|       63.2|        88.5|
|null|Chaitra|        320|       60.1|        75.8|
|   4|  Kamal|       null|       75.0|        null|
|   5| Sohaib|        450|       null|        90.6|
+----+-------+-----------+-----------+------------+

In the second command, where we mentioned `any`, so the rows with atleast 1 null value will be omitted. Thus the output is

+----+----+-----------+-----------+------------+
|Roll|Name|Final Marks|Float Marks|Double Marks|
+----+----+-----------+-----------+------------+
+----+----+-----------+-----------+------------+
```

### How to drop rows where only specific columns have null values?
Similar to the above `na.drop()`, we pass the condition to the `na.drop(<condition>)`
```scala
// omits the rows where "Final Marks" and "Float Marks" have null values
val conditionNull=df.na.drop(Seq("Final Marks", "Float Marks"))
conditionNull.show() 
```
**Output**
```text
+----+-------+-----------+-----------+------------+
|Roll|   Name|Final Marks|Float Marks|Double Marks|
+----+-------+-----------+-----------+------------+
|   2|   null|        350|       63.2|        88.5|
|null|Chaitra|        320|       60.1|        75.8|
+----+-------+-----------+-----------+------------+
```

### How to replace the null values in a dataframe?
We can replace the null values using the `na.fill()` method.
```scala
val fillNull=df.na.fill(0)
fillNull.show()
```
**Output**
```text
All the numeric null values have been replaced with "0"
+----+-------+-----------+-----------+------------+
|Roll|   Name|Final Marks|Float Marks|Double Marks|
+----+-------+-----------+-----------+------------+
|   1|   Ajay|        300|        0.0|       92.75|
|   2|   null|        350|       63.2|        88.5|
|   0|Chaitra|        320|       60.1|        75.8|
|   4|  Kamal|          0|       75.0|         0.0|
|   5| Sohaib|        450|        0.0|        90.6|
+----+-------+-----------+-----------+------------+
```
### How to replace null values of a specific column?
Similar to replacing all the null values, we impose a condition explicitly to fill the null values of a specific column.
```scala
val nameNull=df.na.fill("Unknown", Seq("Name"))
nameNull.show()
```
**Output**
```text
Only the null value in the name column has been replaced with 'Unknown'
+----+-------+-----------+-----------+------------+
|Roll|   Name|Final Marks|Float Marks|Double Marks|
+----+-------+-----------+-----------+------------+
|   1|   Ajay|        300|       null|       92.75|
|   2|Unknown|        350|       63.2|        88.5|
|null|Chaitra|        320|       60.1|        75.8|
|   4|  Kamal|       null|       75.0|        null|
|   5| Sohaib|        450|       null|        90.6|
+----+-------+-----------+-----------+------------+
```
### How to replace multiple null values with different values?
Replacing null values with different values for each column can be done using the Map() method inside na.fill()
```scala
val multiNull=df.na.fill(Map("Final Marks" -> 0, "Float Marks" -> 55.7F, "Name" -> "Unknown"))
multiNull.show()
```
**Output**
```text 
Only the selected column's null values will be replaced 
+----+-------+-----------+-----------+------------+
|Roll|   Name|Final Marks|Float Marks|Double Marks|
+----+-------+-----------+-----------+------------+
|   1|   Ajay|        300|       55.7|       92.75|
|   2|Unknown|        350|       63.2|        88.5|
|null|Chaitra|        320|       60.1|        75.8|
|   4|  Kamal|          0|       75.0|        null|
|   5| Sohaib|        450|       55.7|        90.6|
+----+-------+-----------+-----------+------------+
```
### Summary
Null values in data are inevitable and exist no matter whatever conditions are put. And, null values can interfere with out interpretations as well. We have seen methods to handle null values in spark dataframes. We have looked at
- Finding the rows with null values.
- Filtering rows which have null values in sepcific columns.
- Dropping the rows which have null values.
- Replacing the all the null values.
- Replacing null values of a particular column with a specific value.

### References
- [Null Semantics](https://spark.apache.org/docs/3.5.2/sql-ref-null-semantics.html#comp-operators)
- [Filtering Null Values from dataframe](https://stackoverflow.com/questions/39727742/how-to-filter-out-a-null-value-from-spark-dataframe)
