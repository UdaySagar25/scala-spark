## User Defined Functions(UDFs)

### What are User Defined Functions?
A UDF is a function you write yourself to perform operations on Spark data that aren’t covered by Spark’s standard functions. They’re useful when you need to apply complex transformations, custom computations, or domain-specific logic to your dataframe.
We UDFs in Spark when the required transformation is not available as built-in Spark function. 


### References
- [What are user-defined functions (UDFs)?](https://docs.databricks.com/aws/en/udf/)
- [Scalar User Defined Functions](https://spark.apache.org/docs/3.5.2/sql-ref-functions-udf-scalar.html)
- [Creating User Defined Function in Spark-SQL](https://stackoverflow.com/questions/25031129/creating-user-defined-function-in-spark-sql)


**UDF-1**
- What are UDFs and why use them
- create UDFs
- create UDFs for numeric operations, String operations. 
- single column UDF

**UDF-2**
- Advanced usage of UDFs
- handling null values using UDFs
- multi column UDFs
- map types in UDFs
- testing UDFs