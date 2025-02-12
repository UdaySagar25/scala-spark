import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object dateTime {
  def main(args: Array[String]): Unit ={
    val spark=SparkSession.builder
      .appName("dateTime")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    // Create the DataFrame (same as before)
    val studentData = Seq(
      (1, "Ajay", 55, "2024-09-15"),
      (2, "Bharghav", 63, "2024-09-10"),
      (3, "Chaitra", 60, "2024-09-20"),
      (4, "Kamal", 75, "2024-09-12"),
      (5, "Sohaib", 70, "2024-09-18")
    )

    val df = studentData.toDF("Roll", "Name", "Marks", "EnrollmentDate")
    val dfWithDate = df.withColumn("EnrollmentDate", to_date($"EnrollmentDate"))
//    dfWithDate.show()

    val dfWithComponents = dfWithDate.withColumn("EnrollmentYear", year($"EnrollmentDate"))
      .withColumn("EnrollmentMonth", month($"EnrollmentDate"))
      .withColumn("EnrollmentDay", dayofmonth($"EnrollmentDate"))

//    dfWithComponents.show()

    val moreComponents=dfWithDate.withColumn("DayOfYear", dayofyear($"EnrollmentDate"))
      .withColumn("WeekOfYear", weekofyear($"EnrollmentDate"))
      .withColumn("Quarter", quarter($"EnrollmentDate"))
//    moreComponents.show()

    val dfFormatted = dfWithDate.withColumn("FormattedDate", date_format($"EnrollmentDate", "yyyy-MM-dd")) // ISO 8601 format
      .withColumn("AnotherFormat", date_format($"EnrollmentDate", "MM/dd/yyyy")) // US format
      .withColumn("MonthName", date_format($"EnrollmentDate", "MMMM")) // Full month name

//    dfFormatted.show()

    val dfWithTimestamp = dfWithDate.withColumn("RegistrationTime", current_timestamp())
    dfWithTimestamp.show()

    val dfWithTimeZone = dfWithTimestamp.withColumn("ISTRegistrationTime", from_utc_timestamp($"RegistrationTime", "Asia/Kolkata"))
    dfWithTimeZone.show()

    spark.stop()
  }
}
