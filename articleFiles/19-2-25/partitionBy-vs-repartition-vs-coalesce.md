### partitionBy vs repartition vs coalesce

In the previous articles, we have seen 3 methods which are used to manage the partitions while writing csv files. Each of the methods have their own use cases and pros and cons.
In this article, we will discuss the similarities and differences among the 3 methods and also understand when to use which method.

### When to use `partitionBy()`?
partitionBy()` is used while writing data to csv files. It helps in arranging the data in a clean manner which improves data retrieval during analysis. 
The distribution of the data is done based on the value of specified column(s) 
```scala
df.coalesce(1).write.partitionBy("Age")
      .option("header","true").mode("overwrite")
      .csv("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\partitionFiles")
```
**Output**
```csv
Roll,Name,DOB,Submit Time,Final Marks
3,Chaitra,12/12/2010,2025/02/17 12:45:10,75.8
5,Sohaib,14/04/2009,2025/02/17 12:55:20,90.6
8,Ganesh,30/09/2010,2025/02/17 12:50:30,88.2
11,Jasmine,10/02/2011,2025/02/17 12:05:25,79.5
```
```csv
Roll,Name,DOB,Submit Time,Final Marks
1,Ajay,01/01/2010,2025/02/17 12:30:45,92.7
4,Kamal,25/08/2010,2025/02/17 12:40:05,82.3
6,Divya,18/07/2010,2025/02/17 12:20:15,85.4
9,Hema,05/11/2009,2025/02/17 12:15:45,91.0
12,Kiran,28/06/2009,2025/02/17 12:00:40,93.8
```
```csv
Roll,Name,DOB,Submit Time,Final Marks
2,Bharghav,04/06/2009,2025/02/17 12:35:30,88.5
7,Faisal,23/05/2009,2025/02/17 12:25:50,78.9
10,Ishaan,20/03/2008,2025/02/17 12:10:05,87.6
```

### When to use `repartition()`?
The method `repartition()` is used to shuffle the data across the partitions randomly, which helps in distribution of load evenly for parallel processing. 
This method is used to distribute the data in memory. And, the number of partitions(n) are defined inside the method `repartition(n)`

```scala
df.repartition(5).write
  .option("header", "true").mode("overwrite")
  .csv("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\repartitionFiles")
```
The above spark command distributed the data randomly between the 5 partitions.

### When to use `coalesce()`?
The method `coalesce()` is primarily used to reduce the number of data partitions the disk while writing the data to csv files. This method is used when we want the data together and also when the data is small.
This will in-turn help with proper data analysis.
```scala
df.coalesce(1).write.format("csv")
  .option("header","true")
  .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\singleFile")
```
**Output**
```csv
Roll,Name,Final Marks,Float Marks,Double Marks
1,Ajay,300,55.5,92.75
2,Bharghav,350,63.2,88.5
3,Chaitra,320,60.1,75.8
4,Kamal,360,75.0,82.3
5,Sohaib,450,70.8,90.6
```
Since the data is tiny, it is advised to set coalesce to small number(1,2,3).

### Summary
In this article, we have seen:
- The difference scenarios where partitionBy, repartition and coalesce can be used.
- What are the advantages and disadvantages of partitionBy, repartition and coalesce.

### Related articles
- [Writing CSV Files in Spark](@/docs/spark/writing-csv-files-in-spark.md)
- [Writing CSV files using partitions](@/docs/spark/writing-csv-files-using-partitions.md)

### References
- [Spark - repartition() vs coalesce()](https://stackoverflow.com/questions/31610971/spark-repartition-vs-coalesce)
- [Spark Dataframe repartition](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html)
- [Which situation is it better to use coalesce vs repartition](https://stackoverflow.com/questions/54230109/which-situation-is-it-better-to-use-coalesce-vs-repartition)