### CSV file compression

CSV files, as we know are popular for storing data, and they are simple in terms of use. However, when we have huge amounts of data, the same csv file consumes large space.
To overcome this, spark provided built-in configurations, which can be used during write operations.
In this article, we will explore different methods of file compressions.


Let us consider the dataframe that we used in the previous articles. [Writing CSV files using partitions](@/docs/spark/writing-csv-files-using-partitions.md)
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

Before going ahead with compression operations, we need to know that Spark supports only certain codecs.
- gzip
- bzip2
- lz4
- snappy
- deflate

### How to compress csv files during write operations?
Let us begin our discussion with compressing files using **gzip**. To configure this, we define where `option` method.
```scala
df.write.partitionBy("Age")
      .option("header","true").option("compression","gzip").mode("overwrite")
      .csv("csvFiles\\compressedStudentAgeData")
```
In the output folder, compressed folders will be created, segregated by age of the students.

But when we deeply observe, each csv file will have only 1 record. Now to handle it?

### How to compress csv files with data in a single file?
To answer this question, and solve our problem, we can use the `coalesce()` method, which will reduce the number of partitions.

```scala
df.coalesce(1).write.partitionBy("Age")
      .option("header","true").option("compression","gzip").mode("overwrite")
      .csv("csvFiles\\compressedStudentAgeData")
```
This spark command will execute and create only 1 file for each age group, making it easier to comprehend the data.

To know more about coalesce, refer these articles [Writing CSV files using partitions](@/docs/spark/writing-csv-files-using-partitions.md) and [partitionBy vs repartition vs coalesce](@/docs/spark/partitionBy-vs-repartition-vs-coalesce.md)

The benefit of using `gzip` compression is that, it drastically reduces the file size, making it efficient for storing and exchanges, but it uses more memory for decompression.

### How can I speed up the compression time?
To compress the data faster, without worrying about the storage, we can configure the option `snappy`, which is faster than gzip. It is best used when performance is of priority.
```scala
df.coalesce(1).write.partitionBy("Age")
      .option("header","true").option("compression","snappy").mode("overwrite")
      .csv("csvFiles\\snappyCompressedData")
```

### How to create a zipped file with high compression?
Spark provides another compress option `bzip2()` which is used to compress the dataframes, more than that of gzip.
But the downside is, due to high compression and decompression, it puts a lot of load on the CPU.
```scala
df.coalesce(1).write.partitionBy("Age")
      .option("header","true").option("compression","bzip2").mode("overwrite")
      .csv("csvFiles\\bzipCompressedData")
```

### How can I compress the dataframe making sure that spark takes little time while the compression ratio is also neither high nor low?
`deflate()` processes the dataframe with good balance between speed and compression ratio.

```scala
df.coalesce(1).write.partitionBy("Age")
      .option("header","true").option("compression","deflate").mode("overwrite")
      .csv("csvFiles\\deflateCompressedData")
```
This spark code splits the dataframe based on age of the students and then the files are compressed.


### Summary
In this article, we have seen:
- Types of compressions available in Spark.
- How spark writes a dataframe into csv and compresses it.
- How can coalesce be integrated while compressing a dataframe into csv file.
- How different types of compressions work.
- Pros and Cons of different compression in Spark.

### Related Articles
- [Writing CSV files using partitions](@/docs/spark/writing-csv-files-using-partitions.md)
- [partitionBy vs repartition vs coalesce](@/docs/spark/partitionBy-vs-repartition-vs-coalesce.md)

### References
-[Spark CSV files documentation](https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html)