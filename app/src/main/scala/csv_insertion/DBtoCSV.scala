package csv_insertion

import com.opencsv.CSVWriter

import java.io.FileWriter
import java.sql.{Connection, DriverManager}

object DBtoCSV extends App{

  val url = "jdbc:mysql://localhost:3306/csvinsertion"
  val username = "root"
  val password = "root"
  val driver = "com.mysql.cj.jdbc.Driver"
  Class.forName(driver)
  val connection: Connection = DriverManager.getConnection(url, username, password)

  val startTime = System.currentTimeMillis()

  try{

    connection.setAutoCommit(false)

    val selectStatement = "SELECT * FROM CSVTABLE"
    val state = connection.createStatement()
    val result = state.executeQuery(selectStatement)
    val filepath = "output.csv"
    val csvWriter = new CSVWriter(new FileWriter(filepath))
    val metadata =result.getMetaData
    val columnCount = metadata.getColumnCount
    while (result.next()) {
      val row = (1 to columnCount).map(result.getString).toArray
      csvWriter.writeNext(row)
    }
    csvWriter.close()
    val endTime = System.currentTimeMillis()
    val totalTimeSeconds = (endTime - startTime) / 1000.0
    println(s"Total insertion time: ${totalTimeSeconds} seconds")

  }
  catch {
    case e:Exception => e.printStackTrace()
  }
  finally {
    if (connection != null) connection.close()
  }
}
