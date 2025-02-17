## Handle CSV files of different encodings


Encoding CSV files ensures that the data in the file is correctly interpreted and displayed across systems and different regions.
Encoding determines how the characters are stored and read. Encoding is used to prevent the character corruption, which means that the special characters may become unreadable when wrong encoding is used.
It also helps us to exchange non-English characters, such as Hindi, few German letters, Chinese, and many other languages.

While converting encodings from one type to another, we need to make sure that both are compatible, else there is a possibility that we might lose important information.

Let us now see how to handle the encoding and encoding conversion of CSV Files using spark.

We will be looking about a couple of most commonly used csv encodings.

### How to safely read the files that are encoded?
If you know the encoding of the csv file, then we can directly apply that, else we can apply another encoding which `UTF-8` which is capable of supporting multiple languages nad special characters.

Assume, we have a csv file which is UTF-8 encoded. 

```csv
Name, Nationality, Salary
Jürgen Müller,Germany,£1400
Élodie Durand,France,£1450
José García,Spain,£1395
Åsa Björk,Sweden,£1500
```

Let us try to see what happens if the file is not read with correct encoding.

```scala
val encodeDf= spark.read.option("header","true")
      .option("inferSchema","true")
      .option("encoding","Windows-1252")
      .csv("csvFiles/satScores.csv")
    
encodeDf.show()
encodeDf.printSchema()
```
**Output**
```text
+---------------+------------+-------+
|           Name| Nationality| Salary|
+---------------+------------+-------+
|JÃ¼rgen MÃ¼ller|     Germany| Â£1400|
| Ã‰lodie Durand|      France| Â£1450|
|JosÃ© GarcÃ­a|       Spain| Â£1395|
|    Ã…sa BjÃ¶rk|      Sweden| Â£1500|
+---------------+------------+-------+

root
 |-- Name: string (nullable = true)
 |--  Nationality: string (nullable = true)
 |--  Salary: string (nullable = true)
```

We can see that the names and the Salary column has been inappropriately read. This can lead to data loss and wrong data.

To ensure the data is correctly read and displayed, we have to be specific with the encoding.
Let us try to see how the output is when the encoding is changed to UTF-8

```scala
val encodeDf1= spark.read.option("header","true")
      .option("inferSchema","true")
      .option("encoding","UTF-8")
      .csv("csvFiles/satScores.csv")
    
encodeDf1.show()
encodeDf1.printSchema()
```
**Output**
```text
+-------------+------------+-------+
|         Name| Nationality| Salary|
+-------------+------------+-------+
|Jürgen Müller|     Germany|  £1400|
|Élodie Durand|      France|  £1450|
|  José García|       Spain|  £1395|
|    Åsa Björk|      Sweden|  £1500|
+-------------+------------+-------+

root
 |-- Name: string (nullable = true)
 |--  Nationality: string (nullable = true)
 |--  Salary: string (nullable = true)
```
Now we can observe that the output is being correctly interpreted by the spark. 

Similar to UTF-8, there are other popular encodings which we might encounter while reading data from source files. We need to make sure that the files re tested and then read correctly.
Different Encodings are
- UTF-8; Most commonly used and supports a lot of languages and characters
- UTF-8 with BOM; Used for Excel compatibility on Windows.
- Windows-1252; Encoding used for Western European Languages

### Summary
- In this article, we have seen that what happens if the files are not read with correct encodings. There might be data loss or data representation of data.
- We have also seen about how to read the files, with the specified and appropriate encoding so that the data is interpreted correctly.

### References
- [How to parse CSV file with UTF-8 encoding?](https://stackoverflow.com/questions/44002651/how-to-parse-csv-file-with-utf-8-encoding)
- [Spark CSV documentation](https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html)
- [Read CSV file in pyspark with ANSI encoding](https://stackoverflow.com/questions/59645851/read-csv-file-in-pyspark-with-ansi-encoding)
