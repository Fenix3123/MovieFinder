import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, to_date, col}


object query {
  def main(args:Array[String]): Unit = {
    queryWeighted()

  }
  def queryAll(): Unit ={
    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    spark.sql("Select * from Movies").show()
    spark.sql("Select * from Genres").show()

  }

  def queryOne(name: String): Unit ={
    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    spark.sql(s"SELECT * FROM Movies where title = '$name'").show()
  }
  def queryPopular(): Unit ={
    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    spark.sql("Select * from Movies WHERE popularity = (SELECT MAX(popularity) FROM Movies)").show()
  }

  def queryGenre(name:String): Unit ={
    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    val df1 = spark.read.json(path = "C:\\proj1\\response.json")
    val df2 = spark.read.json(path = "C:\\proj1\\genres.json")

    import spark.implicits._
    val q1 = df1.select($"title", explode($"genre_ids").alias("id") ).toDF()
    q1.createOrReplaceTempView("shows")
    df2.createOrReplaceTempView("genre")

    val q3 = spark.sql("Select * from shows Left join genre where shows.id = genre.id").toDF()
    q3.createOrReplaceTempView("collected")
    spark.sql(s"Select title, name from collected where name = '$name'").show(1000)
  }

  def queryDate(year1:String, year2:String): Unit ={
    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    val df1 = spark.sql("SELECT * FROM Movies")
    import spark.implicits._
    val q1 = df1.select($"title", to_date(col("release_date"), "yyyy-MM-dd").alias("date"))
    q1.createOrReplaceTempView("Calendar")
    spark.sql(s"SELECT * FROM Calendar WHERE date >= '$year1-01-01' AND date < '$year2-12-31' order by date desc").show(1000)
  }

  def queryCount(): Unit ={
    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    val df1 = spark.read.json(path = "C:\\proj1\\response.json")
    val df2 = spark.read.json(path = "C:\\proj1\\genres.json")

    import spark.implicits._
    val q1 = df1.select($"title", explode($"genre_ids").alias("id") ).toDF()
    q1.createOrReplaceTempView("shows")
    df2.createOrReplaceTempView("genre")

    val q3 = spark.sql("Select * from shows Left join genre where shows.id = genre.id").toDF()
    q3.createOrReplaceTempView("collected")
    spark.sql("Select name, count(name) as count from collected group by name order by count desc").show()
  }
  def queryWeighted() ={
    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")
    val df = spark.sql("SELECT title, cast(sum(vote_average * vote_count) / sum(vote_count) as decimal(8,2)) as weighted_average FROM Movies group by title order by weighted_average desc")
    df.show(1000)
  }


}
