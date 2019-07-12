import scala.io.{Source}
import java.io.{File, PrintWriter}
import org.apache.spark._
import org.apache.spark.streaming._


object log {

  def main(args: Array[String]): Unit = {


    //Data to write in File using PrintWriter
    val writer = new PrintWriter(new File("log.txt"))

    val filename = "lorem.txt.txt"
    for (line <- Source.fromFile(filename).getLines) {

      writer.write(line)
      writer.write("\n")

      Thread.sleep(1)
    }

    writer.close()


    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    val conf = new SparkConf().setMaster("local[2]").setAppName("log")

    //the checkpointed data would be rewritten every 10 seconds......checkpoint is nothing but cache but it stores on disk
    val ssc = new StreamingContext(conf, Seconds(50))

    //textFileStream can only monitor a folder when the files in the folder are being added or updated.
    val lines = ssc.textFileStream("/Users/anushamuppalla/Downloads/SparkStreamingScala-2/logs")


    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()


  }
}