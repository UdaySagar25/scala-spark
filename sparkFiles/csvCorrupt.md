import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object csvCorrupt {
  def main(args: Array[String]): Unit={
    val spark = SparkSession.builder()
      .appName("Handling Corrupt and Missing Data")
      .master("local[*]")
      .getOrCreate()

      //define the schema of the csv file
      val schema = StructType(Seq(
        StructField("Roll", IntegerType, true),
        StructField("Name", StringType, true),
        StructField("Final Marks", IntegerType, true),
        StructField("Float Marks", FloatType, true),
        StructField("Double Marks", DoubleType, true)
      ))

    val df=spark.read.schema(schema).option("header","true")
      .csv("corruptData.csv")

//df.show()
    val corruptDf=spark.read.option("header","true")
      .schema(schema)
      .option("mode", "DROPMALFORMED")
      .csv("corruptData.csv")

//    corruptDf.show()

    val schema1 = StructType(Seq(
      StructField("Roll", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Final Marks", IntegerType, true),
      StructField("Float Marks", FloatType, true),
      StructField("Double Marks", DoubleType, true),
      StructField("Bad records",StringType,true)
    ))

    val permissiveDf=spark.read.option("header","true")
      .schema(schema1)
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "Bad records")
      .csv("corruptData.csv")

    permissiveDf.show(truncate=false)

    spark.stop()
  }
}
