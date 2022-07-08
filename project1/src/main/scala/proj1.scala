import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object proj1 {
  def main(args: Array[String]): Unit={
    System.setProperty("hadoop.home.dir", "C:\\hadoop3")
    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")

    val df1 = spark.read.csv("hdfs://localhost:9000/user/will/people.csv")
    df1.createOrReplaceTempView("people")
    spark.sql(sqlText="Select * from people WHERE _c1=23; ").show()

    val df2 = spark.read.format("csv").option("sep",",").load("hdfs://localhost:9000/user/Bev_BranchA.txt")
    df2.createOrReplaceTempView("beverage")
    spark.sql("SELECT * from beverage;").show()
    spark.sql("Select _c0 from beverage WHERE _c1='Branch6'").show()

  }
}
