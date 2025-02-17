import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object encodeCsv {
  def main(args:Array[String]): Unit={
    val spark = SparkSession.builder()
      .appName("csvOps3")
      .master("local[*]")
      .getOrCreate()

    val encodeDf= spark.read.option("header","true")
      .option("inferSchema","true")
      .option("encoding","Windows-1252")
      .csv("csvFiles/satScores.csv")

    encodeDf.show()
    encodeDf.printSchema()

    val encodeDf1= spark.read.option("header","true")
      .option("inferSchema","true")
      .option("encoding","UTF-8")
      .csv("csvFiles/satScores.csv")

    encodeDf1.show()
    encodeDf1.printSchema()


    spark.stop()
  }
}
