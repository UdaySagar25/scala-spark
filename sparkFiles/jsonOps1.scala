import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object jsonOps1 {
  def main(args: Array[String]): Unit={
    val spark=SparkSession.builder()
      .appName("jsonOps1").master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val singleLine=spark.read.json("jsonFiles/singleLine.json")

//    singleLine.show()
//    singleLine.printSchema()

    val multiline=spark.read.json("jsonFiles/students.json")
//    multiline.show()
//    multiline.printSchema()

//    val jsonDf=spark.read.option("multiLine","true").json("jsonFiles/students.json")
//
//    jsonDf.printSchema()
//    jsonDf.show()

    val stdDetails=spark.read.option("multiLine","true").json("jsonFiles/studentDetails.json")

    stdDetails.printSchema()
    stdDetails.show(truncate=false)

    spark.stop()
  }
}
