## Spark JSON file value parsing

Parsing JSON values is as important as parsing the field names and is similar to it as well. But while dealing with large datasets, we often encounter special values and have formatting issues. 
Whether its numerical values, non-numeric floating-point values, or even type inconsistencies, Spark provides us with configurable options that allow us to have control while parsing data.

In this article, let us look at all those configurable options.

### I have records in the JSON file with leading zeros. How to parse the values?
Consider the json file:
```json
[
  {
    "Roll": 001,
    "Name": "Ajay",
    "Marks": 55
  },
  {
    "Roll": 002,
    "Name": "Bharghav",
    "Marks": 63
  },
...
```
Value for the Roll field have numbers starting from 0, which is not accepted by standard JSON format. Parsing this json file will throw an error.
```scala
val leadZero=spark.read.option("multiline","true")
      .option("inferSchema","true")
      .json("jsonFiles/leadZero.json")
    
leadZero.show()
```
**Output**
```text
the queries from raw JSON/CSV files are disallowed when the referenced columns only include the internal corrupt record column
```

To solve this issue, we will use `allowNumericLeadingZeros` option which will ignore the zeros and parse the data correctly
```scala
val leadZero=spark.read.option("multiline","true")
      .option("inferSchema","true")
      .option("allowNumericLeadingZeros","true")
      .json("jsonFiles/leadZero.json")

    leadZero.show()
```
**Outupt**
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

### How to handle escape characters in JSON files?
We usually use backslash(\) as an escape character in JSON files. It is used if in cases where the string value contains double quotes.
It is also used to shift to new line or give tab spaces
Consider the JSON file `backslash.json`
```json
[
  {
    "Roll": 1,
    "Name": "Ajay",
    "Marks": 55,
    "Dialogue": "I am in a \"Safe\" room"
  },
  {
    "Roll": 2,
    "Name": "Bharghav",
    "Marks": 63,
    "Dialogue": "Please don't \tgo there"
  },
...
```
```scala
val backslash=spark.read.option("multiLine","true")
      .option("allowBackslashEscapingAnyCharacter","true")
      .json("jsonFiles/backslash.json")

backslash.show(truncate=false)
```
**Output**
```text
+----------------------------+-----+--------+----+
|Dialogue                    |Marks|Name    |Roll|
+----------------------------+-----+--------+----+
|I am in a "Safe" room       |55   |Ajay    |1   |
|Please don't 	go there     |63   |Bharghav|2   |
|The world is so "beautiful!"|60   |Chaitra |3   |
|I like 	ice-cream        |75   |Kamal   |4   |
|Let's go on a vacation      |70   |Sohaib  |5   |
+----------------------------+-----+--------+----+
```
### I have non-numeric data in my JSON file. How to handle this?
Non-Numeric numbers are generally represented with **NaN**, **Infinity** and **-Infinity**. These values are used when values are really extreme and cannot be expressed numerically.
To handle this case, we will use `allowNonNumericNumbers` and set it to true.

Consider the json file `nonNum.json`

```json
[
  {
    "Roll": 1,
    "Name": "Ajay",
    "Marks": 55
  },
  {
    "Roll": 2,
    "Name": "Bharghav",
    "Marks": NaN
  },
...
```

Before that, we will see, what happens if `allowNonNumericNumbers` is set to false
```scala
val nonNum=spark.read.option("multiLine","true")
      .option("inferSchema","true")
      .option("allowNonNumericNumbers","false")
      .json("jsonFiles/nonNum.json")

nonNum.show()
```
```text
the queries from raw JSON/CSV files are disallowed when the referenced columns only include the internal corrupt record column
```
We can see that Spark throws an error and fails to parse the data. To resolve this, we need to set it to true to ensure the data is parsed correctly.

```scala
val nonNum=spark.read.option("multiLine","true")
      .option("inferSchema","true")
      .option("allowNonNumericNumbers","true")
      .json("jsonFiles/nonNum.json")

nonNum.show()
```
**Output**
```text
+--------+--------+----+
|   Marks|    Name|Roll|
+--------+--------+----+
|    55.0|    Ajay|   1|
|     NaN|Bharghav|   2|
|Infinity| Chaitra|   3|
|     NaN|   Kamal|   4|
|    70.0|  Sohaib|   5|
+--------+--------+----+
```

There are many scenarios, where we want to make note of precise values, for example, precision must be there while constructing something.
But standard JSON reads the decimal values as Double which will affect the data analysis. To make sure that decimal data is read correctly spark has configuration defined.

### How to parse float data value in JSON files?
Spark has the configuration `prefersDecimal` which allows the data to be read in its original form, rather than changing its data type.
```scala
val floatMarks=spark.read.option("multiLine","true")
      .option("inferSchema","true")
      .json("jsonFiles/floatMarks.json")

    floatMarks.printSchema()
```
**Output**
```text
root
 |-- Marks: double (nullable = true)
 |-- Name: string (nullable = true)
 |-- Roll: long (nullable = true)
```

When `prefersDecimal` is set to true, the Marks values will be read as Decimal type
```scala
val floatMarks=spark.read.option("multiLine","true")
      .option("inferSchema","true")
      .option("prefersDecimal","true")
      .json("jsonFiles/floatMarks.json")

floatMarks.printSchema()
```
**Output**
```text
root
 |-- Marks: decimal(11,9) (nullable = true)
 |-- Name: string (nullable = true)
 |-- Roll: long (nullable = true)
```

### Summary
In this article, we have seen:
- How can we parse JSON files which have data with leading zeros.
- How to parse data when there are backslash-escape characters.
- How to handle non-numeric values 
- What is the standard behaviour of spark in parsing decimal numbers and how to handle decimal values.

### References
- [Why is JSON invalid if an integer begins with a leading zero?](http://stackoverflow.com/questions/27361565/why-is-json-invalid-if-an-integer-begins-with-a-leading-zero#:~:text=JSON%20syntax%20doesn't%20allow,put%20your%20numbers%20in%20quotes.)
- [JSON numeric types](https://json-schema.org/understanding-json-schema/reference/numeric)
