import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.rdd.RDD

object createRDD {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("CreateRDD")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Create a DataFrame with column names
    val df: DataFrame = Seq(
      (1, "Ajay", 55),
      (2, "Bharghav", 63),
      (3, "Chaitra", 60),
      (4, "Kamal", 75),
      (5, "Sohaib", 70)
    ).toDF("Roll", "Name", "Marks")

    // Convert DataFrame to RDD[Row]
    val rddFromDF: RDD[Row] = df.rdd

    // Convert RDD[Row] to RDD with specific types
    val typedRDD: RDD[(Int, String, Int)] = rddFromDF.map(row =>
      (row.getAs[Int]("Roll"), row.getAs[String]("Name"), row.getAs[Int]("Marks"))
    )
    // Print the RDD
    typedRDD.collect().foreach(println)

    spark.stop()
  }
}
