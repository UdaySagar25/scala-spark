import org.apache.spark.sql.{SparkSession,Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object structType {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("dateTime")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._

    val studentSchema=StructType(Array(
      StructField("ID",IntegerType,nullable=false),
      StructField("Name",StringType,nullable=false),
      StructField("Marks",IntegerType,nullable=false),
      StructField("Hobbies",StructType(Array(
        StructField("hobby1", StringType, nullable = false),
        StructField("hobby2", StringType, nullable = false)
      )),nullable = true)
    ))

    val sampledata=Seq(
      Row(1, "Ajay",55, Row("Singing", "Sudoku")),
      Row(2, "Bhargav",63, Row("Dancing", "Painting")),
      Row(3, "Chaitra",60, Row("Chess", "Long Walks")),
      Row(4, "Kamal", 75, Row("Reading Books", "Cooking")),
      Row(5, "Sohaib", 70, Row("Singing", "Cooking"))
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(sampledata), studentSchema)

//    df.show()

//    df.select(col("Name"),col("Hobbies.hobby1")).show()

    // renaming the column name
//    df.select(col("Name"), col("Hobbies.hobby1").alias("hobby")).show()

    //flattenning the struct columns
//    df.selectExpr("Name","Hobbies.hobby1 as hobby1","Hobbies.hobby2 as hobby2").show()

    //Updating the nested fields of a struct type
    val updatedDF = df.withColumn("NewColumn", struct(col("Name"), col("Marks")))
    updatedDF.show()

    df.filter(col("Hobbies.hobby1") === "Singing").show()


    spark.stop()
  }
}
