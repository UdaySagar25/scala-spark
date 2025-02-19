## Handling Unescaped quotes in a csv file

Assume we encounter few source data files, and the records in those files contain unescaped quotes. For example, "She said, "hello" to everyone".
Here the whole sentence is said to be one field value, but the inner quotations might cause a problem when parsing the data. How to deal with them?

### How to read csv files, if there unescaped quotes in a field?
Consider the csv file `delimiter.csv`
```csv 
Roll,Name,Marks, Dialouge
1,Ajay,55,"He said "good morning" to the teacher"
2,Bharghav,63,"This is the "locker" in the country"
3,Chaitra,60,"You cannotb be serious"
4,Kamal,75,"I love "apple pie""
5,Sohaib,70,"It is raining outside"
```
Rows 1,2 and 4 have unescaped quotes("good morning", "locker" and "apple pie")

In Spark, we have `unescapedQuoteHandling` which can help us with dealing these kind of situations. And it has multiple options to correctly parse the csv file.

Let us look at how csv data behaves to different options of `unescapedQuoteHandling`

```scala
val dfDelimiter = spark.read
  .option("header", "true")
  .option("unescapedQuoteHandling", "STOP_AT_CLOSING_QUOTE")
  .csv("csvFiles/delimiter.csv")

dfDelimiter.show(truncate = false)
```
**Output**
```text
+----+--------+-----+-------------------------------------+
|Roll|Name    |Marks| Dialouge                            |
+----+--------+-----+-------------------------------------+
|1   |Ajay    |55   |He said "good morning" to the teacher|
|2   |Bharghav|63   |This is the "locker" in the country  |
|3   |Chaitra |60   |You cannot be serious                |
|4   |Kamal   |75   |I love "apple pie"                   |
|5   |Sohaib  |70   |It is raining outside                |
+----+--------+-----+-------------------------------------+
```

```scala
val dfDelimiter1 = spark.read
      .option("header", "true")
      .option("unescapedQuoteHandling", "BACK_TO_DELIMITER")
      .csv("csvFiles/delimiter.csv")

    dfDelimiter1.show(truncate = false)
```
**Output**
```text
+----+--------+-----+--------------------------------------+
|Roll|Name    |Marks| Dialouge                             |
+----+--------+-----+--------------------------------------+
|1   |Ajay    |55   |"He said good morning" to the teacher"|
|2   |Bharghav|63   |"This is the locker" in the country"  |
|3   |Chaitra |60   |You cannot be serious                 |
|4   |Kamal   |75   |"I love apple pie" "                  |
|5   |Sohaib  |70   |It is raining outside                 |
+----+--------+-----+--------------------------------------+
```

```scala 
val dfDelimiter2 = spark.read
  .option("header", "true")
  .option("unescapedQuoteHandling", "STOP_AT_DELIMITER")
  .csv("csvFiles/delimiter.csv")

dfDelimiter2.show(truncate = false)
```
**Output**
```text
+----+--------+-----+---------------------------------------+
|Roll|Name    |Marks| Dialouge                              |
+----+--------+-----+---------------------------------------+
|1   |Ajay    |55   |"He said "good morning" to the teacher"|
|2   |Bharghav|63   |"This is the "locker" in the country"  |
|3   |Chaitra |60   |You cannot be serious                  |
|4   |Kamal   |75   |"I love "apple pie" "                  |
|5   |Sohaib  |70   |It is raining outside                  |
+----+--------+-----+---------------------------------------+
```

We have seen how spark is reading the csv file for every option of `unescapedQuoteHandling`.

In our case, `STOP_AT_CLOSING_QUOTE` is working, but sometimes, the data can be truncated.

On the other hand,`BACK_TO_DELIMITER` and `STOP_AT_DELIMITER` might prevent data truncation, but sometimes using them will lead to data misinterpretation.
So we need to make sure that we are using the correct `unescapedQuoteHandling` option while dealing with large and different kinds of data.


### Summary
- In this article we have seen 
- Wat can be the challenges  with reading data with unescaped quotes.
- How spark methods can help us with dealing the columns with unescaped quotes.
- What are the pros and cons for each of the options of `unescapedQuoteHandling`

### Related articles
- [CSV file operations](csvOps1.md)
- [CSV file schema handling](csvOps2.md)

### References
- [Spark CSV documentation](https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html)
- [Spark Read CSV doesn't preserve the double quotes while reading!](https://community.databricks.com/t5/data-engineering/spark-read-csv-doesn-t-preserve-the-double-quotes-while-reading/td-p/27086)
