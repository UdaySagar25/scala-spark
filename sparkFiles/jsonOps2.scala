import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object jsonOps2 {
  def main(args: Array[String]): Unit={

    val spark=SparkSession.builder().appName("JSON file operations")
      .master("local[*]").getOrCreate()


    val stdMarks=spark.read.option("multiline","true")
      .option("inferSchema","true")
      .json("jsonFiles/studentMarks.json")

//    stdMarks.printSchema()

    val ownSchema = StructType(Seq(
      StructField("Roll", ShortType, true),
      StructField("Name", StringType, true),
      StructField("Final Marks", IntegerType, true),
      StructField("Float Marks", FloatType,true),
      StructField("Double Marks", DoubleType,true)
    ))

    val schMarks=spark.read.option("multiline","true").schema(ownSchema)
      .json("jsonFiles/studentMarks.json")

    schMarks.printSchema()

    val sch = StructType(Seq(
      StructField("Roll", ShortType, true),
      StructField("Name", StringType, true),
      StructField("Final Marks", IntegerType, true),
      StructField("Contact", StructType(Seq(
        StructField("Mail", StringType, true),
        StructField("Mobile", StringType, true)
      )), true)
    ))

    val nestedSchema=spark.read.option("multiline","true")
      .schema(sch).json("jsonFiles/studentDetails.json")

    nestedSchema.printSchema()



    spark.stop()
  }
}
