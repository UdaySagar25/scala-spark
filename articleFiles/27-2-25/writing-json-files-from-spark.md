### Writing JSON Files from Spark

In all the previous articles, we have seen different ways, probable issues while reading a JSON file. 
Now we shall look into writing a JSON file in spark.

Consider we have a dataframe
```text
+----+--------+-----------+
|Roll|    Name|Final Marks|
+----+--------+-----------+
|   1|    Ajay|        300|
|   2|Bharghav|        350|
|   3| Chaitra|        320|
|   4|   Kamal|        360|
|   5|  Sohaib|        450|
+----+--------+-----------+
```
We shall see how to write the above dataframe into JSON files in Spark.

### How to write a JSON file?
In spark, to write a JSON file, we use the `.write` method, along with `json()` where we give the path of the directory. This creates a directory containing JSON file.
```scala
df.write.json("jsonFiles\\studentData")
```
This above functions creates **studentData** directory inside the root directory **jsonFiles**. By default, this function creates n-JSON files where 'n' is the number of records.

### How to write all the records in a single JSON file?
```scala
df.coalesce(1).write
  .json("jsonFiles\\combinedStudentData")
```
Upon executing the above function, Spark creates a single JSON file having the all the records.

**Output**
```json
{"Roll":1,"Name":"Ajay","Final Marks":300}
{"Roll":2,"Name":"Bharghav","Final Marks":350}
{"Roll":3,"Name":"Chaitra","Final Marks":320}
{"Roll":4,"Name":"Kamal","Final Marks":360}
{"Roll":5,"Name":"Sohaib","Final Marks":450}
```

### How to write a JSON file with nested dataframe?
Writing a JSON file with nested dataframe is similar to writing a simple JSON file. Spark is capable of parsing the nested dataframe into a nested JSON object.

Consider we have a dataframe 
```text
+----+--------+-----+-------------------------------+
|Roll|Name    |Marks|Contact                        |
+----+--------+-----+-------------------------------+
|1   |Ajay    |55   |[[ajay@Mail.com, 8973 113]]    |
|2   |Bharghav|63   |[[bharghav@Mail.com, 9876 205]]|
|3   |Chaitra |60   |[[chaitra@Mail.com, 7789 656]] |
|4   |Kamal   |75   |[[kamal@Mail.com, 8867 325]]   |
|5   |Sohaib  |70   |[[sohaib@Mail.com, 9546 365]]  |
+----+--------+-----+-------------------------------+
```
To write this into a JSON file, we will execute the below spark code
```scala
nestedDf.coalesce(1).write
  .json("jsonFiles\\studentDetails")
```
**Output**
```json
{"Roll":1,"Name":"Ajay","Marks":55,"Contact":[{"Mail":"ajay@Mail.com","Mobile":"8973 113"}]}
{"Roll":2,"Name":"Bharghav","Marks":63,"Contact":[{"Mail":"bharghav@Mail.com","Mobile":"9876 205"}]}
{"Roll":3,"Name":"Chaitra","Marks":60,"Contact":[{"Mail":"chaitra@Mail.com","Mobile":"7789 656"}]}
{"Roll":4,"Name":"Kamal","Marks":75,"Contact":[{"Mail":"kamal@Mail.com","Mobile":"8867 325"}]}
{"Roll":5,"Name":"Sohaib","Marks":70,"Contact":[{"Mail":"sohaib@Mail.com","Mobile":"9546 365"}]}
```
We can see that the JSON object has been created with nested JSON objects.

### How can write a JSON file in the existing path?
By default, if we give a file path which already exists during file write, Spark session will throw an error. For example
```scala
nestedDf.coalesce(1).write
  .json("jsonFiles\\studentDetails")
```
**Output**
```text
path file:jsonFiles/studentDetails already exists.
```
To overcome this, we will use the set the option `overwrite` , which will overwrite the existing data.
```scala
nestedDf.coalesce(1).write
  .mode("overwrite")
  .json("jsonFiles\\studentDetails")
```

### How to add another JSON file to an already existing path?
We can set the option `append` which will allow the spark to create a new JSON file in the already existing path.
```scala
nestedDf.coalesce(1).write
  .mode("append") 
  .json("jsonFiles\\combinedStudentData")
```
**Output**
```json
{"Roll":1,"Name":"Ajay","Marks":55,"Contact":[{"Mail":"ajay@Mail.com","Mobile":"8973 113"}]}
{"Roll":2,"Name":"Bharghav","Marks":63,"Contact":[{"Mail":"bharghav@Mail.com","Mobile":"9876 205"}]}
{"Roll":3,"Name":"Chaitra","Marks":60,"Contact":[{"Mail":"chaitra@Mail.com","Mobile":"7789 656"}]}
{"Roll":4,"Name":"Kamal","Marks":75,"Contact":[{"Mail":"kamal@Mail.com","Mobile":"8867 325"}]}
{"Roll":5,"Name":"Sohaib","Marks":70,"Contact":[{"Mail":"sohaib@Mail.com","Mobile":"9546 365"}]}
```

### How can I write a JSON file with null values?
Consider the dataframe 
```text
+----+--------+-----------+
|Roll|    Name|Final Marks|
+----+--------+-----------+
|   1|    Ajay|       null|
|   2|Bharghav|        350|
|   3| Chaitra|        320|
|   4|    null|        360|
|   5|  Sohaib|        450|
+----+--------+-----------+
```
In the above dataframe, we have a couple of null values, here and there. Spark will ignore the null value fields, leading to data loss.
```scala
newDf.coalesce(1).write
  .mode("overwrite")
  .json("jsonFiles\\corruptStudentData")
```
**Output**
```json
{"Roll":1,"Name":"Ajay"}
{"Roll":2,"Name":"Bharghav","Final Marks":350}
{"Roll":3,"Name":"Chaitra","Final Marks":320}
{"Roll":4,"Final Marks":360}
{"Roll":5,"Name":"Sohaib","Final Marks":450}
```

To overcome this, we will set the option `ignoreNullFields` to False which will parse the null value fields as well
```scala
newDf.coalesce(1).write
  .mode("overwrite")
  .option("ignoreNullFields","false")
  .json("jsonFiles\\corruptStudentData")
```
**Output**
```json
{"Roll":1,"Name":"Ajay","Final Marks":null}
{"Roll":2,"Name":"Bharghav","Final Marks":350}
{"Roll":3,"Name":"Chaitra","Final Marks":320}
{"Roll":4,"Name":null,"Final Marks":360}
{"Roll":5,"Name":"Sohaib","Final Marks":450}
```

### Summary
In this article, we have seen:
- How a JSON file is written from a dataframe.
- The default behavior of Spark while writing a JSON file.
- How we can have all the records in one JSON file.
- How to overwrite and append existing JSON files.
- How we can create a nested JSON object from a nested dataframe.
- How we can parse dataframes which have null values in it.

### Related Articles
- [JSON file null and corrupt values parsing](@/docs/spark/json-file-null-and-corrupt-value-parsing.md)
- [Handling JSON files in Spark](@/docs/spark/handling-json-files-in-spark.md)

### References
- [Save the contents of SparkDataFrame as a JSON file](https://spark.apache.org/docs/3.5.4/api/R/reference/write.json.html) 
- [JSON File Documentation](https://spark.apache.org/docs/3.5.4/sql-data-sources-json.html)
- [Pyspark dataframe write to single json file with specific name](https://stackoverflow.com/questions/43269244/pyspark-dataframe-write-to-single-json-file-with-specific-name)