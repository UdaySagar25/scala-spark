import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object DataTypes {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataTypes")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Creating DataFrame directly
    val df = Seq(
      (1.toShort, "Ajay", 'A'.toString, 10L, "2010-01-01", 55.5F, 92.75, true, Seq("Singing", "Sudoku")),
      (2.toShort, "Bharghav", 'B'.toString, 20L, "2009-06-04", 63.2F, 88.5, false, Seq("Dancing", "Painting")),
      (3.toShort, "Chaitra", 'C'.toString, 30L, "2010-12-12", 60.1F, 75.8, true, Seq("Chess", "Long Walks")),
      (4.toShort, "Kamal", 'D'.toString, 40L, "2010-08-25", 75.0F, 82.3, false, Seq("Reading Books", "Cooking")),
      (5.toShort, "Sohaib", 'E'.toString, 50L, "2009-04-14", 70.8F, 90.6, true, Seq("Singing", "Cooking"))
    ).toDF("ID", "Name", "Grade", "LongValue", "DOB", "FloatMarks", "DoubleMarks", "IsActive", "Hobbies")

    // Convert DOB column from String to DateType
    val dfWithDate = df.withColumn("DOB", to_date($"DOB", "yyyy-MM-dd"))

    dfWithDate.show(false)
    dfWithDate.printSchema()

    spark.stop()
  }
}
