import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object csvOps3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("csvOps3")
      .master("local[*]")
      .getOrCreate()


    val dateDf=spark.read.option("header","true")
      .csv("csvFiles/studentDate.csv")

//    println("DF-1")
//    dateDf.show()
//    dateDf.printSchema()

    val dateDf1=spark.read.option("header","true")
      .option("inferSchema","true")
      .option("dateFormat","yyyy-MM-dd")
      .csv("csvFiles/studentDate.csv")

//    println("DF-2")
//    dateDf1.show()
//    dateDf1.printSchema()

    val dateDf2 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "yyyy:MM:dd")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .csv("csvFiles/studentDate.csv")

//    println("Updated CSV File")
//    dateDf2.show()
//    dateDf2.printSchema()


    val dateDf3 = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "dd-MM-yyyy")
        .option("timestampFormat", "yyyy/MM/dd HH:mm:ss")
      .csv("csvFiles/studentDate2.csv")

    println("New CSV file")
    dateDf3.show()
    dateDf3.printSchema()

    spark.stop()
  }
}
