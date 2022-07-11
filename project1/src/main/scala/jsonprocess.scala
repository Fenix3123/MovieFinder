import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._

object jsonprocess extends App{
  System.setProperty("hadoop.home.dir", "C:\\hadoop3")
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  println("created spark session")
  val df1 = spark.read.json(path = "hdfs://localhost:9000/user/proj1/response.json")
  df1.printSchema()
  df1.createOrReplaceTempView("Shows")
  spark.sql("Select * from Shows")

  val df2 = spark.read.json(path = "hdfs://localhost:9000/user/proj1/genresresponse.json")
  df2.printSchema()
  df2.createOrReplaceTempView("types")
  spark.sql("Select * from types")

  spark.sql("Drop table if exists Movies")
  spark.sql("create table if not exists Movies(adult Boolean, backdrop_path STRING,genre_ids ARRAY<Int>, id Int, original_language string,original_title String, overview String, popularity Double, poster_path String, release_date String, title String, video boolean, vote_average double, vote_count Int)")
  spark.sql("Insert Into Table Movies(Select adult,backdrop_path, genre_ids, id, original_language, original_title, overview, popularity, poster_path, release_date, title, video, vote_average, vote_count from Shows)")
  spark.sql("Select * from Movies").show(1000)

  spark.sql("Set hive.exec.dynamic.partition.mode=nonstrict")
  spark.sql("Drop table if exists MoviesPartitioned")
  spark.sql("create table if not exists MoviesPartitioned(adult Boolean, backdrop_path STRING,genre_ids ARRAY<Int>, id Int, original_title String, overview String, popularity Double, poster_path String, release_date String, title String, video boolean, vote_average double, vote_count Int) PARTITIONED BY(original_language String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
  spark.sql("Insert overwrite Table MoviesPartitioned PARTITION(original_language) (Select adult,backdrop_path, genre_ids, id, original_title, overview, popularity, poster_path, release_date, title, video, vote_average, vote_count, original_language from Shows)")
  spark.sql("Select * from MoviesPartitioned").show(1000)

  val dfs1 = spark.sql("Select * from Movies")
  dfs1.write.mode("overwrite").json("hdfs://localhost:9000/user/proj1/movies.json")

  spark.sql("Drop table if exists Genres")
  spark.sql("create table if not exists Genres(id Int, name String)")
  spark.sql("Insert into Table Genres (Select id, name from types)")
  spark.sql("Select * from Genres").show()

  val dfs2 = spark.sql("Select * from Genres")
  dfs1.write.mode("overwrite").json("hdfs://localhost:9000/user/proj1/genres.json")
  /*val df2 = spark.read.json(path = "C:\\proj1\\genres.json")
  df2.show(1000)*/

  /*val schema = new StructType()
    .add("adult", BooleanType, true)
    .add("backdrop_path", StringType, true)
    .add("genres_id", ArrayType(LongType), true)
    .add("id", LongType, true)
    .add("original_language", StringType, true)
    .add("original_title", StringType, true)
    .add("overview", StringType, true)
    .add("popularity", DoubleType, true)
    .add("poster_path", StringType, true)
    .add("release_date", StringType, true)
    .add("title", StringType, true)
    .add("video", BooleanType, true)
    .add("vote_average", DoubleType, true)
    .add("vote_count", LongType, true)

  val df_with_schema = spark.read.schema(schema).json("hdfs://localhost:9000/user/proj1/response.json")
  df_with_schema.printSchema()
  df_with_schema.show(false)*/

  /*spark.sparkContext.setLogLevel("ERROR")
  spark.sql("DROP table IF EXISTS MoviesTB")
  spark.sql("create table IF NOT EXISTS MoviesTB(adult Boolean,backdrop_path String, genre_ids list, id Int, original_language String, original_title String, overview String, popularity float, poster_path String, release_date String, title String, video boolean, vote_average float, vote_count Int) row format delimited fields terminated by ','")
  spark.sql("LOAD DATA LOCAL INPATH 'Bev_BranchA.txt' INTO TABLE BevA")
  spark.sql("SELECT Count(*) AS TOTALCOUNT FROM BevA").show()*/
}
