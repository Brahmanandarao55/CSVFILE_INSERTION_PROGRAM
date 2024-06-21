package csv_insertion

import java.io.PrintWriter
import scala.util.Random

object GenerateRandomData {

  case class Person(Id: Int, Name: String, Age: Int)

  def main(args: Array[String]): Unit = {
    val outputFile = "data.csv"
    val numRecords = 1000

    // Create a list of potential IDs
    val potentialIds = (1 to numRecords).toList

    // Shuffle the list for randomized unique ID assignment
    val shuffledIds = Random.shuffle(potentialIds)

    // Create a PrintWriter to write to the output file
    val writer = new PrintWriter(outputFile)

    // Write header to the CSV file
    writer.println("Id,Name,Age")

    // Generate and write random data with unique IDs
    shuffledIds.zipWithIndex.foreach { case (id, index) =>
      val name = getRandomName()
      val age = Random.nextInt(100) // Generate random age (adjust max age if needed)

      writer.println(s"$id,$name,$age")
    }

    writer.close()
    println(s"Generated $numRecords records with unique IDs in $outputFile")
  }

  // Function to generate random names
  def getRandomName(): String = {
    val names = Array("Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack")
    val randomIndex = Random.nextInt(names.length)
    names(randomIndex)
  }
}
