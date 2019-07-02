/**
  * Illustrates flatMap + countByValue for wordcount.
  */


import org.apache.spark._

object charcount {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","F:\\winutils" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)


    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("input.txt")
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    val chars = words.flatMap(line1 => line1.split(""))
    println(chars.collect())



    // Transform into word and count.
    val counts = chars.map(line => (line, 1)).reduceByKey{case (x, y) => x + y}
    //val counts = words.map(character => (character, 1)).sortByKey(true,1)
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("output")

    val mapFile = input.flatMap(lines => lines.split(" ")).filter(value => value=="hi")
    println(mapFile.count())
    mapFile.saveAsTextFile("output1")

    val result = input.distinct()

    println(result.collect().mkString(","))
    result.saveAsTextFile("output2")
  }
}
