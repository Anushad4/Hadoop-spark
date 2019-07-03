import org.apache.spark.{ SparkConf, SparkContext }



object SecondarySort {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\winutils" )
    val conf = new SparkConf().setAppName("secondarysort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile("input2.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => ((k(0), k(1)),k(3))}
    println("pairsRDD")
    pairsRDD.foreach { println }
    val numReducers = 2;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(k => k)).sortByKey(true,1)
    println("listRDD")


    listRDD.saveAsTextFile("Output7");

  }
}