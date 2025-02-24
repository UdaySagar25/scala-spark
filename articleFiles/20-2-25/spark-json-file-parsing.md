## Spark JSON File Parsing

JSON files are a form of storing and exchanging data, and handling JSON files is crucial, especially when the amount of data is large as there might be possibilities of having data that does not follow standard JSON formats.

To tackle this, Spark has a variety of configuration options that allows us to process the data efficiently.

In this article, we shall look into few json configuration methods that help us read json files seamlessly.

### The field names are enclosed in single quotes(' '). How to read JSON files with field names enclosed in single quotes?
In JSON files, field names are supposed to be enclosed in double quotes(" "). When they are enclosed in single quotes(' '), spark session throws an error.
To tackle it, we use `allowSingleQuotes`.

Consider the JSON file `singleQuote.json`
```json
[
  {
    "Roll": 1,
    'Name': "Ajay",
    "Marks": 55
  },
  {
    'Roll': 2,
    "Name": "Bharghav",
    "Marks": 63
  },
...
```

Let us see what happens when `allowSingleQuotes` is set to false.
```scala
val stdMarks=spark.read.option("multiline","true")
      .option("inferSchema","true")
      .option("allowSingleQuotes","false")
      .json("jsonFiles/singleQuote.json")

stdMarks.show()

```
**Output**
```text
the queries from raw JSON/CSV files are disallowed when the referenced columns only include the internal corrupt record column
```
As I said earlier, spark throws an error. Now lets see what happens if `allowSingleQuotes` is set to true.
```scala
val stdMarks=spark.read.option("multiline","true")
      .option("inferSchema","true")
      .option("allowSingleQuotes","true")
      .json("jsonFiles/singleQuote.json")

stdMarks.show()
```
**Output**
```text
+-----+--------+----+
|Marks|    Name|Roll|
+-----+--------+----+
|   55|    Ajay|   1|
|   63|Bharghav|   2|
|   60| Chaitra|   3|
|   75|   Kamal|   4|
|   70|  Sohaib|   5|
+-----+--------+----+
```
Now the JSON file is being parsed correctly. It is better to set `allowSingleQuotes` to true when we are dealing with large files.

### My JSON files has field names that are not enclosed in quotes, both single and double. How to deal with JSON files with unquoted field names?
This is a common issue because there are instances when the file authors forget to put field names inside quotes unintentionally.
So to properly parse the files, we will use `allowUnquotedFieldNames`. This will ignore the error and parse the JSON file.

Consider the JSON file `unQuoted.json`
```json
[
  {
    "Roll": 1,
     Name: "Ajay",
    "Marks": 55
  },
  {
     Roll: 2,
    "Name": "Bharghav",
    "Marks": 63
  },
...
```
```scala
val unquote=spark.read.option("multiline","true")
      .option("inferSchema","true")
      .json("jsonFiles/unQuoted.json")

unquote.show()
```
Executing the above spark command, will throw the error
```text
the queries from raw JSON/CSV files are disallowed when the referenced columns only include the internal corrupt record column
```
We will see the results after using `allowUnquotedFieldNames`.
```scala
val unquote=spark.read.option("multiline","true")
  .option("inferSchema","true")
  .option("allowUnquotedFieldNames","true")
  .json("jsonFiles/unQuoted.json")

unquote.show()
```
**Output**
```text
+-----+--------+----+
|Marks|    Name|Roll|
+-----+--------+----+
|   55|    Ajay|   1|
|   63|Bharghav|   2|
|   60| Chaitra|   3|
|   75|   Kamal|   4|
|   70|  Sohaib|   5|
+-----+--------+----+
```
### How can I read JSON files which has comments in it?
Consider we have a JSON file `commentMarks.json`
```json
[
  {
    "Roll": 1,
    "Name": "Ajay",
    "Marks": 55   //These is the lowest score
  },
  {
    "Roll": 2,
    "Name": "Bharghav",
    "Marks": 63
  },
...
```

In few cases, the authors of JSON files tend to write a comment about the data, helping others to understand the data better. But when we try to read those JSON file, we get an error.
To handle this situation, we will use `allowComments` option. This ignores the comments and reads the data.

Let's see what error we get in spark when we read a data with comments in it.
```scala
val commentMarks=spark.read.option("multiline","true")
      .option("inferSchema","true")
      .json("jsonFiles/commentMarks.json")

commentMarks.show()
```
```text
the queries from raw JSON/CSV files are disallowed when the referenced columns only include the internal corrupt record column
```
Let's see how the output will be if `allowComments` is set to true.
```scala
val commentMarks=spark.read.option("multiline","true")
  .option("inferSchema","true")
  .option("allowComments","true")
  .json("jsonFiles/commentMarks.json")

commentMarks.show()
```
**Output**
```text
+-----+--------+----+
|Marks|    Name|Roll|
+-----+--------+----+
|   55|    Ajay|   1|
|   63|Bharghav|   2|
|   60| Chaitra|   3|
|   75|   Kamal|   4|
|   70|  Sohaib|   5|
+-----+--------+----+
```
When `allowComments` is set to true, spark ignores the comments and read the JSON files. It is advised to have `allowComments` set to true when we are dealing with large files.


### Summary
In this article, we have looked into:
- Common issues faced while reading a JSON file.
- Reading JSON files when the field names are enclosed in single quotes(' ') and not enclosed.
- Reading JSON files when there are comments in the files.

### References
- [Spark JSON documentation](https://spark.apache.org/docs/3.5.4/sql-data-sources-json.html)
