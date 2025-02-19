## Writing CSV Files in Spark


Until now, we have looked in to various ways to read a csv file, explicitly defining custom schema to read a csv file, selecting only fields that are required to answer the problem statement.
Now let us look into ways to write a csv file in spark. We will also look into potential challenges and try to find a way to solve them.

### How can I write a dataframe in to a csv file?
Spark provides us with methods that convert the dataframe format of data into csv file. 
The usual way of creating a csv file is that Spark creates a directory first and then splits the data into creating multiple csv files.

Consider we have a dataframe which we want to write into a csv file.
```text
+----+--------+-----------+-----------+------------+
|Roll|    Name|Final Marks|Float Marks|Double Marks|
+----+--------+-----------+-----------+------------+
|   1|    Ajay|        300|       55.5|       92.75|
|   2|Bharghav|        350|       63.2|        88.5|
|   3| Chaitra|        320|       60.1|        75.8|
|   4|   Kamal|        360|       75.0|        82.3|
|   5|  Sohaib|        450|       70.8|        90.6|
+----+--------+-----------+-----------+------------+
```
To write the above dataframe into a csv file, we use the method `.write`, followed by a folder path to store the csv files inside `save("file:///<abosuluteFilePath>")`
```scala
df.write.format("csv")
  .option("header","true")
  .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\outputFiles1")
```
I am saving the csv files in `csvFiles/outputFiles1` directory. As I said earlier, the dataframe data will be split and each split will be converted into a csv file. In our case, we'll be having 5 csv files, each file storing 1 row of dataframe.

But this is not the correct way of representing the above data. It might be useful to have multiple csv files created when the amount of data is huge. 
In our case, the data is tiny. So how can we get the data in one csv file?

### How can I write the dataframe into a single csv file?
Spark has a method `coalesce()` which will help us define the number of csv files we need.
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
When we run the above spark command, a single csv file with all the records in it, is created.
`coalesce(n)` helps in creating the desired number of csv files. The maximum number of splits we can create out of a dataframe is the total number of records it holds.

### How to update the already existing csv file?
Sometimes, we might want to update the output file,folder with new data. But spark doesn't allow that and throws an error when executed. How do we tackle this situation?
```scala
df.coalesce(2).write.format("csv")
  .option("header","true")
  .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\outputFiles1")
```
When the above spark code is executed, spark will raise an error saying that the file path already exists.
**Output**
```text
path file:/C:/Users/krisa/OneDrive/Desktop/spark-articles/csvFiles/outputFiles1 already exists.
```
To tackle this situation, we will use `mode("overwrite")` configuration which will overwrite the existing folder
```scala
df.coalesce(2).write.format("csv")
      .option("header","true")
      .mode("overwrite")
      .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\outputFiles1")
```
Now we'll get 2 csv files created in the folder, overwriting the already existing list of files.
**Output**
```csv
Roll,Name,Final Marks,Float Marks,Double Marks
1,Ajay,300,55.5,92.75
2,Bharghav,350,63.2,88.5
```
```csv
Roll,Name,Final Marks,Float Marks,Double Marks
3,Chaitra,320,60.1,75.8
4,Kamal,360,75.0,82.3
5,Sohaib,450,70.8,90.6
```

### Is it possible to write a csv file with a custom delimiter?
We know that it is possible to read a csv file which has a delimiter other than the default comma(,). Similarly, we can even write a csv file with a custom delimiter.
```scala
df.coalesce(1).write.format("csv")
      .option("header","true")
      .option("delimiter","|")
      .mode("overwrite")
      .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\customDelimiter")
```
**Output**
```csv
Roll|Name|Final Marks|Float Marks|Double Marks
1|Ajay|300|55.5|92.75
2|Bharghav|350|63.2|88.5
3|Chaitra|320|60.1|75.8
4|Kamal|360|75.0|82.3
5|Sohaib|450|70.8|90.6
```
We can even use another delimiter as well such as, ("." , ";" , " ")

### How to append new csv files to the existing folders?
It is also possible to add new files to the existing folder. For that, we will use `mode("append")` configuration. This configuration will not disturb the existing files, and add the new files to the directory.
```scala
df.coalesce(1).write.format("csv")
      .option("header","true")
      .option("delimiter",";")
      .mode("append")
      .save("file:///C:\\Users\\krisa\\OneDrive\\Desktop\\spark-articles\\csvFiles\\customDelimiter")
```
**Output**
The below csv file is created and stored in the `customDelimiter` along with the other files.
```csv
Roll;Name;Final Marks;Float Marks;Double Marks
1;Ajay;300;55.5;92.75
2;Bharghav;350;63.2;88.5
3;Chaitra;320;60.1;75.8
4;Kamal;360;75.0;82.3
5;Sohaib;450;70.8;90.6
```

### Summary
In this article, we have seen:
- How can we write a csv file from a spark dataframe.
- What is the default behaviour of spark in writing csv files.
- How to write the spark dataframe into a single csv file.
- How to overwrite the files of an existing directory.
- How to write a csv file with a custom delimiter.
- How to append new csv files to the existing directory.

### References
- [Dataframe Writer to CSV](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.csv.html)
- [How to write the resulting RDD to a csv file in Spark python](https://stackoverflow.com/questions/31898964/how-to-write-the-resulting-rdd-to-a-csv-file-in-spark-python)