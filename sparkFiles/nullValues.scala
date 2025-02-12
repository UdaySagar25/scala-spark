import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.

object nullValues {
  def main(args: Array[String]): Unit={
    val spark=SparkSession.builder()
      .appName("nullValues")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val schema = StructType(Seq(
      StructField("Roll", IntegerType, true),
      StructField("Name", StringType, true),
      StructField("Final Marks", IntegerType, true),
      StructField("Float Marks", FloatType, true),
      StructField("Double Marks", DoubleType, true)
    ))

    val data = Seq(
      Row(1, "Ajay", 300, null, 92.75),
      Row(2, null, 350, 63.2F, 88.5),
      Row(null, "Chaitra", 320, 60.1F, 75.8),
      Row(4, "Kamal", null, 75.0F, null),
      Row(5, "Sohaib", 450, null, 90.6)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
//    df.show()

    val allNull=df.na.drop("all") // this omits the rows where all the values are null
//    allNull.show()

    val fewNull=df.na.drop("any")// this omits the rows which has atleast 1 null value
//    fewNull.show()

    val conditionNull=df.na.drop(Seq("Final Marks", "Float Marks"))
//    conditionNull.show() // omits the rows where "Final Marks" and "Float Marks" have null values

    val fillNull=df.na.fill(0)
//    fillNull.show()

    val nameNull=df.na.fill("Unknown", Seq("Name"))
//    nameNull.show()

    val multiNull=df.na.fill(Map("Final Marks" -> 0, "Float Marks" -> 55.7F, "Name" -> "Unknown"))
//    multiNull.show()

    val aggrNull=df.na.fill(Map(
      "Final Marks" -> df.agg(min("Final Marks")),
      "Float Marks" -> df.agg(min("Float Marks")),
      "Double Marks" -> df.agg(min("Double Marks"))
    ))
    aggrNull.show()

    spark.stop()
  }
}
