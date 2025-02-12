## DataFrame Row Operations

Let's use the previously created data frame which we created during Data frame column operations. [Refer this to create dataframes](scala/DFColumn.md)

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
### How to add a new row to an existing dataframe?
Spark dataframes are immutable, which implies that we cannot directly add new rows to existing dataframe. Instead, we have to create a new dataframe and combine both using `.union(<new DF name>)` operation.
```scala
val newRow=Seq((6,"Tanmay", 77)).toDF("Roll", "Name", "Marks")
val updatedDF=df.union(newRow)
```
**Outupt**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   55|
|   2|Bharghav|   63|
|   3| Chaitra|   60|
|   4|   Kamal|   75|
|   5|  Sohaib|   70|
|   6|  Tanmay|   77|
+----+--------+-----+
```
### How to add multiple new rows to an existing dataframe?
Adding multiple rows to dataframe is similar to how we add a single row to the dataframe
```scala
val multipleRows=Seq((7,"Tina",67),
      (8,"Utkarsh",65)).toDF("Roll", "Name", "Marks")
val moreRows=updatedDF.union(multipleRows)
```
**Outupt**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   55|
|   2|Bharghav|   63|
|   3| Chaitra|   60|
|   4|   Kamal|   75|
|   5|  Sohaib|   70|
|   6|  Tanmay|   77|
|   7|    Tina|   67|
|   8| Utkarsh|   65|
+----+--------+-----+
```
### How to delete rows from the dataframe?
As we already know that dataframes are immutable, deleting rows from it directly is not possible.
Instead, what we can do is that we can create a copy of dataframe without the rows that are not required.

```scala
// the below line executes and displays all the rows of dataframe with Marks greater than 63
val delRow = df.filter(!($"Marks" <=63))
delRow.show()
```
**Output**
```text
+----+------+-----+
|Roll|  Name|Marks|
+----+------+-----+
|   4| Kamal|   75|
|   5|Sohaib|   70|
+----+------+-----+
```
There is another alternate way to delete rows from the dataframe, using `expr()` method from spark sql.
```scala
import org.apache.spark.sql.functions._
val rmRows = updatedDF.filter(expr("Marks % 2 = 0"))
val newDf=updatedDF.except(rmRows)
newDf.show()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   55|
|   2|Bharghav|   63|
|   4|   Kamal|   75|
|   6|  Tanmay|   77|
+----+--------+-----+
```
### How to get the distinct rows of the dataframe?
We can get the distinct rows from the dataframe using the below method
```scala
moreRows.distinct().show()
```

### How to sort the rows of a dataframe?
We can use `orderBy()` method to sort the dataframe based on a particular column values
```scala
val descendingOrder=moreRows.orderBy(col("Marks").desc)
descendingOrder.show()

val ascendingOrder=moreRows.orderBy(col("Marks").asc)
ascendingOrder.show()
```
**Output**
```text
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   6|  Tanmay|   77|
|   4|   Kamal|   75|
|   5|  Sohaib|   70|
|   7|    Tina|   67|
|   8| Utkarsh|   65|
|   2|Bharghav|   63|
|   3| Chaitra|   60|
|   1|    Ajay|   55|
+----+--------+-----+


+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   55|
|   3| Chaitra|   60|
|   2|Bharghav|   63|
|   8| Utkarsh|   65|
|   7|    Tina|   67|
|   5|  Sohaib|   70|
|   4|   Kamal|   75|
|   6|  Tanmay|   77|
+----+--------+-----+
```

## Summary 
Throughout this article, we have learnt about the row operations in apache spark dataframes. We have learnt,
- Add rows to a dataframe
- Filter the rows of dataframe
- Delete rows from a dataframe
- Get the distinct rows of a dataframe
- Sort the rows of the dataframe

## References
- [How to loop through each of dataframe](https://stackoverflow.com/questions/36349281/how-to-loop-through-each-row-of-dataframe-in-pyspark)
- [How to sort dataframes?](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.sort.html)
