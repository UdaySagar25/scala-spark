import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
object EmptyDataframe {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("EmptyDataFrame")
      .master("local[*]")
      .getOrCreate()

    // Define the schema
    val schema = StructType(Seq(
      StructField("Roll", IntegerType, nullable = true),
      StructField("Name", StringType, nullable = true),
      StructField("Marks", IntegerType, nullable = true)
    ))
    // Create an Empty DataFrame
    val emptyDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], schema)

    // Show Empty DataFrame
    emptyDF.show()

    // Print Schema
    emptyDF.printSchema()

    //alternate way to print the schema of the created DataFrame
    println(emptyDF.schema)

    // Show Empty DataFrame
    emptyDF.show()

    // Stop Spark Session
    spark.stop()
  }
}


