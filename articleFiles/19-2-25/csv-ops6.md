### Writing CSV files using partitions

As much as we want the data to be in a single file, sometimes we feel that data should have been segregated based on a condition so that it would be easy to find records.

### How to split the data by column ?
To do this, we can use `partitionBy` method, which will split the data by column value, rather than having them split randomly.

Consider we have a dataframe
```scala
val df=Seq(
        (1, "Ajay", 14, "01/01/2010", "2025/02/17 12:30:45", 92.7),
        (2, "Bharghav", 15, "04/06/2009", "2025/02/17 12:35:30", 88.5),
        (3, "Chaitra", 13, "12/12/2010", "2025/02/17 12:45:10", 75.8),
        (4, "Kamal", 14, "25/08/2010", "2025/02/17 12:40:05", 82.3),
        (5, "Sohaib", 13, "14/04/2009", "2025/02/17 12:55:20", 90.6),
        (6, "Divya", 14, "18/07/2010", "2025/02/17 12:20:15", 85.4),
        (7, "Faisal", 15, "23/05/2009", "2025/02/17 12:25:50", 78.9),
        (8, "Ganesh", 13, "30/09/2010", "2025/02/17 12:50:30", 88.2),
        (9, "Hema", 14, "05/11/2009", "2025/02/17 12:15:45", 91.0),
        (10, "Ishaan", 15, "20/03/2008", "2025/02/17 12:10:05", 87.6),
        (11, "Jasmine", 13, "10/02/2011", "2025/02/17 12:05:25", 79.5),
        (12, "Kiran", 14, "28/06/2009", "2025/02/17 12:00:40", 93.8)
    ).toDF("Roll", "Name", "Age","DOB","Submit Time","Final Marks")
```
Now we want to partition this based on the age of the students
```scala
df.write.partitionBy("Age")
      .option("header","true").mode("overwrite")
      .csv("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\partitionFiles")
```
This will create subdirectories for each age that is present in the dataframe.

When we open the subdirectories, you see that for each record, a unique file has been created. To handle this situation we can
use `coalesce(1)` to put all the records into a single file.
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

### How to partition the data, based on a clause?
Assume we want to segregate the students who got more marks than the cutoff mark in the exam. To do this, we will use `when()` and `otherwise()` methods.
```scala
val cutOff=90.0
    val marksDf=df.withColumn("Pass/Fail",
      when(col("Final Marks")<cutOff,"Pass").otherwise("Fail"))

    marksDf.coalesce(1).write.partitionBy("Pass/Fail")
      .option("header","true").mode("overwrite")
      .csv("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\resutls")
```
**Output**
List of Passed students
```csv
Roll,Name,Age,DOB,Submit Time,Final Marks
1,Ajay,14,01/01/2010,2025/02/17 12:30:45,92.7
5,Sohaib,13,14/04/2009,2025/02/17 12:55:20,90.6
9,Hema,14,05/11/2009,2025/02/17 12:15:45,91.0
12,Kiran,14,28/06/2009,2025/02/17 12:00:40,93.8
```
List of Failed students
```csv
Roll,Name,Age,DOB,Submit Time,Final Marks
2,Bharghav,15,04/06/2009,2025/02/17 12:35:30,88.5
3,Chaitra,13,12/12/2010,2025/02/17 12:45:10,75.8
4,Kamal,14,25/08/2010,2025/02/17 12:40:05,82.3
6,Divya,14,18/07/2010,2025/02/17 12:20:15,85.4
7,Faisal,15,23/05/2009,2025/02/17 12:25:50,78.9
8,Ganesh,13,30/09/2010,2025/02/17 12:50:30,88.2
10,Ishaan,15,20/03/2008,2025/02/17 12:10:05,87.6
11,Jasmine,13,10/02/2011,2025/02/17 12:05:25,79.5
```

There is another way to partition the data, using `repartition()`. It is used to optimize the data distribution across the clusters.
It is used to define the number of partitions and **randomly** distributes data across the partitions.

```scala
df.repartition(5).write
      .option("header", "true").mode("overwrite")
      .csv("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\repartitionFiles")
```

### Summary
In this article, we have seen:
- What is `partitionBy()` method.
- How is it used to distribute the records of csv file into multiple files.
- How can a clause be used to distribute the data and write into different csv files.
- What is `repartition()` and why it is used.
- What are the features of `repartition()`

### References  
-[Write Spark dataframe as CSV with partitions](https://stackoverflow.com/questions/37509932/write-spark-dataframe-as-csv-with-partitions)
- [Dataframe Repartitions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html)
