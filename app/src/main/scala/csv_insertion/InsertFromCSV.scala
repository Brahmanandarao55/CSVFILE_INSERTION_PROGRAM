package csv_insertion

import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement}

object InsertFromCSV extends App {

  // Database connection parameters
  val url = "jdbc:mysql://localhost:3306/csvinsertion"
  val username = "root"
  val password = "root"
  val driver = "com.mysql.cj.jdbc.Driver"

  // Establish database connection
  Class.forName(driver)
  val connection: Connection = DriverManager.getConnection(url, username, password)

  // Path to your CSV file
  val csvFilePath = "C:\\Users\\Brahmananda Rao\\Desktop\\CSV_INSERTION\\app\\src\\main\\scala\\csv_insertion\\csvdata.csv"
  val startTime = System.currentTimeMillis()
  try {
    // Read data from CSV file
    val bufferedSource = scala.io.Source.fromFile(new File(csvFilePath))
    val lines = bufferedSource.getLines().drop(1) // Skip header if exists

    // Prepare SQL statement
    val insertStatement = "INSERT INTO csvtable (ID, NAME, AGE) VALUES (?, ?, ?)"
    val preparedStatement = connection.prepareStatement(insertStatement)

    // Iterate over lines and insert data
    lines.foreach { line =>
      val fields = line.split(",").map(_.trim)
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt

      preparedStatement.setInt(1, id)
      preparedStatement.setString(2, name)
      preparedStatement.setInt(3, age)

      preparedStatement.executeUpdate()
    }

    println("Data inserted successfully!")
  } catch {
    case e: Exception =>
      println(s"Error inserting data: ${e.getMessage}")
  } finally {
    // Close resources
    if (connection != null) connection.close()

    val endTime = System.currentTimeMillis()
    val totalTimeSeconds = (endTime - startTime) / 1000.0
    println(s"Total insertion time: ${totalTimeSeconds} seconds")
  }
}

