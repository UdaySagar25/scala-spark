## Essential Joins in Spark

A join brings two sets of data together by comparing the values of one or more keys of both data and joining them based on the evaluated results.
A join expression determines whether two rows should join and the join types determine what data should be in the result set.

There are many types of join available in spark for us to use. In this article, we will start the journey with **Essential Spark Joins**

Let us start our discussion by creating dataframe. We have already seen how to create dataframes. To refresh you memory, refer [Dataframes](@/docs/spark/dataframes.md) article.

Consider we have the following dataframes.
```text
DataFrame-1
+----+--------+-----+
|Roll|    Name|Marks|
+----+--------+-----+
|   1|    Ajay|   55|
|   2|Bharghav|   63|
|   3| Chaitra|   60|
|   4|   Kamal|   75|
|   5|  Sohaib|   70|
+----+--------+-----+

DataFrame-2
+----+----------------+---+
|Roll|         Subject|Age|
+----+----------------+---+
|   1|       Chemistry| 14|
|   2|Computer Science| 15|
|   5|    BioChemistry| 16|
|   8|      Statistics| 14|
|   7|   Real Analysis| 15|
+----+----------------+---+
```
### Inner Join
When inner join is performed on 2 dataframes, the resulting dataframe would have the rows that have common key.
In the above 2 dataframes, Roll:[1,2,5] are common, so the details of those from both the tables will be joined.
```scala
val innerJoin=df1.join(df2,Seq("Roll"),"inner")
innerJoin.show()
```
**Output**
```text
+----+--------+-----+----------------+---+
|Roll|    Name|Marks|         Subject|Age|
+----+--------+-----+----------------+---+
|   1|    Ajay|   55|       Chemistry| 14|
|   2|Bharghav|   63|Computer Science| 15|
|   5|  Sohaib|   70|    BioChemistry| 16|
+----+--------+-----+----------------+---+
```

### Full Outer Join
When full outer join is performed, all the rows from both dataframes, irrespective of whether they have common keys or not, will be combined. 
This is best applied when we want data from both dataframes to be in a single dataframe.
```scala
val outerJoin=df1.join(df2,Seq("Roll"),"outer")
outerJoin.show()
```
**Outupt**
```text
+----+--------+-----+----------------+----+
|Roll|    Name|Marks|         Subject| Age|
+----+--------+-----+----------------+----+
|   1|    Ajay|   55|       Chemistry|  14|
|   3| Chaitra|   60|            null|null|
|   5|  Sohaib|   70|    BioChemistry|  16|
|   4|   Kamal|   75|            null|null|
|   8|    null| null|      Statistics|  14|
|   7|    null| null|   Real Analysis|  15|
|   2|Bharghav|   63|Computer Science|  15|
+----+--------+-----+----------------+----+
```

### Left Outer Join
When left outer join is done on the two dataframes, all the rows from the left side dataframe are considered and from the right side dataframe, only the rows which have common keys are joined.
The rows that do not match the key from the right side dataframe are omitted.
```scala
val leftOuterJoin=df1.join(df2,Seq("Roll"),"left")
leftOuterJoin.show()
```
**Outupt**
```text
+----+--------+-----+----------------+----+
|Roll|    Name|Marks|         Subject| Age|
+----+--------+-----+----------------+----+
|   1|    Ajay|   55|       Chemistry|  14|
|   2|Bharghav|   63|Computer Science|  15|
|   3| Chaitra|   60|            null|null|
|   4|   Kamal|   75|            null|null|
|   5|  Sohaib|   70|    BioChemistry|  16|
+----+--------+-----+----------------+----+
```

### Right Outer Join
When right outer join is done on the two dataframes, all the rows from the right side dataframe are considered and from the left side dataframe, only the rows which have common keys are joined.
The rows that do not match the key from the left side dataframe are omitted.
```scala
val rightOuterJoin=df1.join(df2,Seq("Roll"),"right")
rightOuterJoin.show()
```
**Output**
```text
+----+--------+-----+----------------+---+
|Roll|    Name|Marks|         Subject|Age|
+----+--------+-----+----------------+---+
|   1|    Ajay|   55|       Chemistry| 14|
|   2|Bharghav|   63|Computer Science| 15|
|   5|  Sohaib|   70|    BioChemistry| 16|
|   8|    null| null|      Statistics| 14|
|   7|    null| null|   Real Analysis| 15|
+----+--------+-----+----------------+---+
```

### Summary
In this article, we have seen:
- Joins and the advantages of joins
- What are the basic essential joins
- How the joins happen based on the type of join.

### Related Articles
- [Dataframes](@/docs/spark/dataframes.md)

### References
- [Apache Spark Joins](https://spark.apache.org/docs/3.5.3/sql-ref-syntax-qry-select-join.html)
- [Spark SQL Join: how it actually works](https://stackoverflow.com/questions/42865178/spark-sql-join-how-it-actually-works)
- [Working with joins on Databricks](https://docs.databricks.com/aws/en/transform/join)
