

import java.io.{File, PrintWriter,FileWriter}

import org.apache.spark._
object file {

  def main(args: Array[String]):Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils")


    //spark context
    val conf = new SparkConf().setMaster("local[2]").setAppName("my app")
    val sc = new SparkContext(conf)

    import scala.io.Source

    val source = Source.fromFile("lorem.txt.txt")
    val lines = source.getLines().toList

    var a = 1

    while (a <= 30) {

      val listlen = lines.length

      //randomly generating text into list
      val RandomGen =scala.util.Random
      val listNumber= RandomGen.nextInt(listlen)

      //Data to write in File using PrintWriter
      val fileWriter = new PrintWriter(new File("logs/log"+ a  +".txt"))

      for (line <- listNumber to listlen-1) {
        fileWriter.write(lines(line))

      }

      fileWriter.close()

      Thread.sleep(5000)
      a +=1


    }
  }
}
