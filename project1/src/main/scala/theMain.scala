import scala.io.StdIn
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import java.sql.{Connection, DriverManager}
import scala.util.control.Breaks.break

object theMain{
  private var connection:Connection = _
  def main(args:Array[String]): Unit ={

    while(true){
      println("Welcome to MovieFinder!!!")
      println("What would you like to do today? (Type a number and enter to execute)")
      println("1. Login")
      println("2. Create a new account")
      println("3. Exit program")
      println("Enter in a number: ")
      var option:Integer = null
      try {
        option = StdIn.readInt()
      }catch{
        case e: NumberFormatException =>{
          println()
          println("---Input has to be one of the numbers---")
          println()
        }
      }
      if(option == 1){
        var username = StdIn.readLine("What is your username?: ")
        var password = StdIn.readLine("What is your password?: ")
        if(checkUser(username: String, password: String) == false) {
          //check for role
          if(getUser(username, password) == true){
            var while1 = true
            while(while1) {
              println()
              println("Welcome " + username)
              println("What would you like to do?")
              println("1. Update Credentials")
              println("2. Delete user")
              println("3. Logout")
              println("4. Query All tables")
              println("5. Look up a specific movie")
              println("6. Find the most popular movie")
              println("7. Find a Movie based on genre ex. (Action, Adventure..)")
              println("8. Looking up Movies between years ex. (2008-2010)")
              println("9. Counting the amount of Movies in Genre")
              println("10. Looking up ratings based on users average rating and vote amount")
              println("Enter in a number: ")
              var option:Integer = null
              try {
                option = StdIn.readInt()
              }catch{
                case e: NumberFormatException =>{
                  println()
                  println("---Input has to be one of the numbers---")
                  println()
                }
              }
              if(option == 1){
                var name = StdIn.readLine("Enter the new name: ")
                var username2 = StdIn.readLine("Enter the new username: ")
                var password = StdIn.readLine("Enter the new password: ")
                updateUser(name, username2, password, username)
                break
              }else if(option == 2){
                deleteUser(username: String)
                break
              }else if(option == 3){
                println("logging out")
                println()
                while1 = false
              }else if(option == 4){
                query.queryAll()
              }else if(option == 5){
                println("What movie would you like to look up")
                var name = StdIn.readLine()
                query.queryOne(name)
              }else if(option == 6){
                query.queryPopular()
              }else if(option == 7){
                println("What movie would you like to look up?: ")
                var name = StdIn.readLine()
                query.queryGenre(name)
              }else if(option == 8){
                println("What is the first year?: ")
                var year1 = StdIn.readLine()
                println("What is the Second year?: ")
                var year2 = StdIn.readLine()
                query.queryDate(year1, year2)
              }else if(option == 9){
               query.queryCount()
              }else if(option == 10){
                query.queryWeighted()
              }
            }//end of while loop for user
          }else{
            var while1 = true
            while(while1) {
              println()
              println("Welcome admin " + username)
              println("What would you like to do?")
              println("1. Update Credentials")
              println("2. Delete admin")
              println("3. Delete a user account")
              println("4. Logout")
              println("Enter a number:")
              var option:Integer = null
              try {
                option = StdIn.readInt()
              }catch{
                case e: NumberFormatException =>{
                  println()
                  println("---Input has to be one of the numbers---")
                  println()
                }
              }
              if(option == 1){
                var name = StdIn.readLine("Enter the new name: ")
                var username2 = StdIn.readLine("Enter the new username: ")
                var password = StdIn.readLine("Enter the new password: ")
                updateUser(name, username2, password, username)
                break
              }else if(option == 2){
                deleteUser(username: String)
                break
              }else if(option == 3){
                println("username of the account to be deleted: ")
                var user_name = StdIn.readLine()
                deleteUser(user_name: String)
              }else if(option == 4){
                println("logging out")
                while1 = false
              }
            }//end of while admin
          }// end of block for admin


        }else {
          println("---------------")
          println("No user exists")
          println("please try again")
          println("---------------")
        }
        //System.exit(0)
      }else if(option == 2){
        println("Creating new account")

        var name = StdIn.readLine("Enter a Name: ")
        var username = StdIn.readLine("Enter a Username: ")
        var password = StdIn.readLine("Enter a password: ")
        makeUser(name: String, username: String, password: String)

      }else if(option == 3){
        println("Exit program")
        System.exit(0)
      }

    }//end of first while true
  }//end of main

  def checkUser(username: String, password: String): Boolean = {
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      //.config("spark.driver.allowMultipleContexts","true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    //values for sql connection
    val url = "jdbc:mysql://localhost:3306/proj1"
    val user = "root"
    val pass = "Setiawan112!"
    //this one prints the whole table based on select * from users
    val sql = "select * from users where username ='"+username+"' And password ='"+password+"'"
    val sourceDf2 = spark.read.format("jdbc").option("url", url)
      .option("dbtable", s"( $sql ) as t").option("user", user)
      .option("password", pass).load()
    return sourceDf2.isEmpty
  }

  def makeUser(name: String, username: String, pass: String): Unit = {
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      //.config("spark.driver.allowMultipleContexts","true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    //values for sql connection
    val url = "jdbc:mysql://localhost:3306/proj1"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val password = "Setiawan112!"
    //this one prints the whole table based on select * from users
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, password)
    var insertSql = connection.prepareStatement(s"Insert into users (name, username, password, role) VALUES ('$name', '$username', '$pass', 'user')")
    println("User successfully created")
    insertSql.executeUpdate()
  }//end of ake user

  def getUser(username:String, password:String): Boolean = {
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      //.config("spark.driver.allowMultipleContexts","true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    val url = "jdbc:mysql://localhost:3306/proj1"
    val user = "root"
    val pass = "Setiawan112!"
    //this one prints the whole table based on select * from users
    val sql = s"select * from users where username ='$username' And password ='$password'"
    val sourceDf2 = spark.read.format("jdbc").option("url", url)
      .option("dbtable", s"( $sql ) as t").option("user", user)
      .option("password", pass).load()
    val getsql = sourceDf2.select("role").collect()
    getsql.foreach(data =>{
      var str = data.mkString
      if(str.equals("user")){
        return true
      }else{
        return false
      }
    })
    return false
  }//end of get user

  def updateUser(name: String, username: String, pass: String, oldUsername: String): Unit ={
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      //.config("spark.driver.allowMultipleContexts","true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    //values for sql connection
    val url = "jdbc:mysql://localhost:3306/proj1"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val password = "Setiawan112!"
    //this one prints the whole table based on select * from users
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, password)
    var insertSql = connection.prepareStatement(s"UPDATE users set name='$name', username='$username',password='$pass' where username = '$oldUsername'")
    println("User successfully updated")
    insertSql.executeUpdate()
  }

  def deleteUser(username: String): Unit ={
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      //.config("spark.driver.allowMultipleContexts","true")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    //values for sql connection
    val url = "jdbc:mysql://localhost:3306/proj1"
    val driver = "com.mysql.cj.jdbc.Driver"
    val user = "root"
    val password = "Setiawan112!"
    //this one prints the whole table based on select * from users
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, password)
    var insertSql = connection.prepareStatement(s"DELETE FROM users WHERE username ='$username'")
    println()
    println("User deleted")
    println()
    insertSql.executeUpdate()
  }

}//end of object


