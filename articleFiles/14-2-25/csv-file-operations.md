## CSV file operations

created on: 14-2-2025

updated on: 18-2-2025

The data in csv files are stored in different formats.
- Records can be stored in single line with a delimiter.
- Records stored in a single line can be be separated using other delimiters as well(",", "|", ";")
- Records can be stores in multiple lines.

Spark provides us with multiple options to handle each and every scenario of csv data format. Let us now see each one of them

### How to handle csv file with multiline data?
There might be scenarios where the data in csv files are stored in multiple lines. spark provides us with a flag `multiline` which will parse the data correctly.
Default is set to `false`

Consider the csv file `multiline.csv`
```csv
Roll,Name,Marks, Description
1,Ajay,55,"He loves
to sing"
2,Bharghav,63,"He is a
basketball player"
3,Chaitra,60,"She is the best
person for debate competitions"
4,Kamal,75, "He is the topper of the class"
5,Sohaib,70,"He is the son of
Vice Principal"
```
Let us see how the data is parsed if `multiline` is not set or set to `false`
```scala
val df=spark.read.option("header","true")
      .csv("csvFiles/multiline.csv")
df.show(truncate=false)
```
**Output**
```text
+-------------------------------+--------+-----+--------------------------------+
|Roll                           |Name    |Marks| Description                    |
+-------------------------------+--------+-----+--------------------------------+
|1                              |Ajay    |55   |He loves                        |
|to sing"                       |NULL    |NULL |NULL                            |
|2                              |Bharghav|63   |He is a                         |
|basketball player"             |NULL    |NULL |NULL                            |
|3                              |Chaitra |60   |She is the best                 |
|person for debate competitions"|NULL    |NULL |NULL                            |
|4                              |Kamal   |75   |He is the topper of the class   |
|5                              |Sohaib  |70   |He is the son of                |
|Vice Principal"                |NULL    |NULL |NULL                            |
+-------------------------------+--------+-----+--------------------------------+
```
We can see that every new line is read as separate record.

This is taken care of if `multiline` is set to `true`
```scala
val multilineDf=spark.read.option("header","true").option("multiline","true")
      .csv("csvFiles/multiline.csv")

multilineDf.show(truncate =false)
```

**Output**
```text
+----+--------+-----+-----------------------------------------------+
|Roll|Name    |Marks| Description                                   |
+----+--------+-----+-----------------------------------------------+
|1   |Ajay    |55   |He loves\nto sing                              |
|2   |Bharghav|63   |He is a\nbasketball player                     |
|3   |Chaitra |60   |She is the best\nperson for debate competitions|
|4   |Kamal   |75   |He is the topper of the class                  |
|5   |Sohaib  |70   |He is the son of\nVice Principal               |
+----+--------+-----+-----------------------------------------------+
```

### How to read csv files with different line separators?
Line separators in csv differ with respect to how they are defined.
There are 3 types of line separators,
- CR (\r) - Line separators used in Older and Classic MAC OS 
- LF (\n) - Line separator used in Linux, and MAC OS
- CRLF (\r\n) - Line separator used in Windows

Let us now see how to deal with each of the line separators
we have `students.csv` defined in `CRLF` and `students2.csv` defined in `LF`.
```csv
Roll,Name,Marks
1,Ajay,55
2,Bharghav,63
3,Chaitra,60
4,Kamal,75
5,Sohaib,70
```
Assume we read the `students2.csv` file which is originally written with `\n` line separator, with `\r\n` separator. The output would be
```scala
val lineSepCsv=spark.read.option("header","true").option("lineSep","\r\n")
      .csv("csvFiles/students2.csv")

lineSepCsv.show()
```
**Output**
```text
+----+----+--------+----+-----+--------+-----+-------+-----+-----+-----+------+---+
|Roll|Name|Marks\n1|Ajay|55\n2|Bharghav|63\n3|Chaitra|60\n4|Kamal|75\n5|Sohaib| 70|
+----+----+--------+----+-----+--------+-----+-------+-----+-----+-----+------+---+
+----+----+--------+----+-----+--------+-----+-------+-----+-----+-----+------+---+
```
We can see that the output is vague and wrong.

Let us now define it with correct line separator, and see the output
```scala
val lineSepCsv1=spark.read.option("header","true").option("lineSep","\n")
      .csv("csvFiles/students2.csv")

lineSepCsv1.show()
```
**Output**
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
We can see that the csv file is being read correctly.

### Summary
In this article, we have seen 
- How to handle csv files with multiline data using `multiLine` flag.
- How to read csv files, that have different line separators.
- What happens if the csv file is not read correctly with proper line separator.

### Related articles
- [Handling CSV files in spark](handling-csv-format-files.md)

### References 
- [How to read multiline csv file as spark dataframe](https://learn.microsoft.com/en-us/answers/questions/1319024/how-to-read-this-multiline-csv-file-as-a-spark-dat)
- [Handling csv files using spark](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)
