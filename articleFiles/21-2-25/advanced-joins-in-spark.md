## Advanced Joins in Spark

In the previous article, we have explored different essential joins which help us to get a better understanding of how joins happen and are foundation to some of the advanced Spark joins.
To refresh your memory about basic joins, refer [Essential Joins in Spark](@/docs/spark/essential-joins-in-spark.md)
Now, lets dive into the next level: Advanced joins in Spark. These joins are used, when it comes to filtering data and generate cartesian product. 
Getting to know these joins will optimize our Spark queries for better data extraction.

Consider the below dataframes
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

DataFrame-3
+----+------+-------------+
|Roll|  Name|      Subject|
+----+------+-------------+
|   4| Kamal|      Biology|
|   5|Sohaib| BioChemistry|
|   7|Tanmay|Real Analysis|
|   8| Tanuj|   Statistics|
+----+------+-------------+
```

### Cross Join
This join is also known as Cartesian Product of two dataframes. 
In this join, each Row of left side database is mapped to each row of right side database
```scala
val cJoin=df3.crossJoin(df2)
cJoin.show()
```
**Output**
```text
+----+------+-------------+----+----------------+---+
|Roll|  Name|      Subject|Roll|         Subject|Age|
+----+------+-------------+----+----------------+---+
|   4| Kamal|      Biology|   1|       Chemistry| 14|
|   4| Kamal|      Biology|   2|Computer Science| 15|
|   4| Kamal|      Biology|   5|    BioChemistry| 16|
|   4| Kamal|      Biology|   8|      Statistics| 14|
|   4| Kamal|      Biology|   7|   Real Analysis| 15|
|   5|Sohaib| BioChemistry|   1|       Chemistry| 14|
|   5|Sohaib| BioChemistry|   2|Computer Science| 15|
|   5|Sohaib| BioChemistry|   5|    BioChemistry| 16|
|   5|Sohaib| BioChemistry|   8|      Statistics| 14|
|   5|Sohaib| BioChemistry|   7|   Real Analysis| 15|
------
```
And the list of combinations go on until the last record from left side dataframe is mapped to the last record of the right side dataframe.

### Left Semi Join
During a Left Semi Join, only the rows from the left DataFrame that have a matching key in the right DataFrame are returned.
The result only includes columns from the left DataFrame. This join is performed to check for existence of a record in another dataframe.
```scala
val leftSemi=df3.join(df2,Seq("Subject"),"left_semi")
leftSemi.show()
```
**Output**
```text
+-------------+----+------+
|      Subject|Roll|  Name|
+-------------+----+------+
| BioChemistry|   5|Sohaib|
|Real Analysis|   7|Tanmay|
|   Statistics|   8| Tanuj|
+-------------+----+------+
```

### Left Anti Join
When a Left Anti Join is performed, it returns only the rows from the left DataFrame that do not have a matching key in the right DataFrame.
This join is performed to identify records that are present in one dataframe but are absent in the other.
```scala
val leftAnti=df3.join(df2,Seq("Subject"),"left_anti")
leftAnti.show()
```
**Output**
```text
+-------+----+-----+
|Subject|Roll| Name|
+-------+----+-----+
|Biology|   4|Kamal|
+-------+----+-----+
```

### Summary
In this article, we have seen:
- Advanced joins which help in filtering, along with joining two dataframes.
- Cross Join(Cartesian Product), Left Semi Join, Left Anti Join.
- How these joins help us with efficient data extraction.

### Related Articles
- [Essential Joins in Spark](@/docs/spark/essential-joins-in-spark.md)

### References
- [Left Semi Join](https://learn.microsoft.com/en-us/kusto/query/join-leftsemi?view=microsoft-fabric)
- [Joins](https://docs.snowflake.com/en/sql-reference/constructs/join)
- [What are the uses of Cross Join?](https://stackoverflow.com/questions/219716/what-are-the-uses-for-cross-join)
- [Natural Join](https://docs.oracle.com/javadb/10.8.2.2/ref/rrefsqljnaturaljoin.html#:~:text=A%20NATURAL%20JOIN%20is%20a,The%20default%20is%20INNER%20join.)
- [Left Anti Join](https://learn.microsoft.com/en-us/power-query/merge-queries-left-anti)
