import org.apache.spark.sql.{DataFrame, SparkSession, Dataset}

case class StudentMarks(Roll: Int, Name: String, Marks: Int) // Move outside main method

object DFtoDS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("DFtoDS")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Create a DataFrame
    val df: DataFrame = Seq(
      (1, "Ajay", 55),
      (2, "Bharghav", 63),
      (3, "Chaitra", 60),
      (4, "Kamal", 75),
      (5, "Sohaib", 70)
    ).toDF("Roll", "Name", "Marks")

    // Convert DataFrame to Dataset[StudentMarks]
    val ds: Dataset[StudentMarks] = df.as[StudentMarks]

    // Show the Dataset
    ds.show()
  }
}
