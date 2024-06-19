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
  val csvFilePath = "C:\\Users\\Brahmananda Rao\\Desktop\\Check\\CSVFILE_INSERTION_PROGRAM\\app\\src\\main\\scala\\csv_insertion\\data.csv"

  val startTime = System.currentTimeMillis()
  try {
    // Disable auto-commit for faster inserts
    connection.setAutoCommit(false)

    // Read data from CSV file (skip header if exists)
    val bufferedSource = scala.io.Source.fromFile(new File(csvFilePath))
    val lines = bufferedSource.getLines().drop(1)  // Skip header

    // Prepare SQL statement
    val insertStatement = "INSERT INTO csvtable (ID, NAME, AGE) VALUES (?, ?, ?)"
    val preparedStatement = connection.prepareStatement(insertStatement)

    // Batch size for inserts
    val batchSize = 1000

    var count = 0
    lines.foreach { line =>
      val fields = line.split(",").map(_.trim)
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt

      preparedStatement.setInt(1, id)
      preparedStatement.setString(2, name)
      preparedStatement.setInt(3, age)
      preparedStatement.addBatch()

      count += 1

      // Execute batch every 'batchSize' records
      if (count % batchSize == 0) {
        preparedStatement.executeBatch()
      }
    }
    // Execute remaining batch
    preparedStatement.executeBatch()

    // Commit transaction
    connection.commit()

    println("Data inserted successfully!")
  } catch {
    case e: Exception =>
      println(s"Error inserting data: ${e.getMessage}")
      // Rollback transaction on error
      connection.rollback()
  } finally {
    // Close resources
    if (connection != null) connection.close()

    val endTime = System.currentTimeMillis()
    val totalTimeSeconds = (endTime - startTime) / 1000.0
    println(s"Total insertion time: ${totalTimeSeconds} seconds")
  }
}
