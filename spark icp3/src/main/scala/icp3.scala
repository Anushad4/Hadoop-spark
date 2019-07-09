import org.apache.spark.sql.SparkSession
import org.apache.spark._

object icp3 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")


    //spark context
    val conf = new SparkConf().setMaster("local[2]").setAppName("my app")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._


    //val df = spark.read.csv("/Users/anushamuppalla/Downloads/survey.csv")
    val df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/anushamuppalla/Downloads/survey.csv")


    //using union
    df.show(numRows = 100)
    df.printSchema()
    val df1 = df.limit(5)
    df1.show()
    val df2 = df.limit(10)
    df2.show()
    val unionDf = df1.union(df2)
    unionDf.show()
    unionDf.orderBy("Country").show()
    df.createOrReplaceTempView("people")
    val DupDF = spark.sql("select COUNT(*),Country from people GROUP By Country Having COUNT(*) > 1")
    DupDF.show()

    //data to file
    println("\n Saved data to file")
    unionDf.write.parquet("/Users/anushamuppalla/Downloads/SparkDataframe-2/temp-paraquet ")


    //query based on group by using treatment column
    val treatment = spark.sql("select count(Country),treatment from people GROUP BY treatment ")
    treatment.show(numRows = 30)

    //13th Row from DataFrame
    val df13th = df.take(13).last
    print(df13th)



    //Aggregate Max and Average
    val MaxDF = spark.sql("select Max(Age) from people")
    MaxDF.show()

    val AvgDF = spark.sql("select Avg(Age) from people")
    AvgDF.show()


    //join query
    val df3 = df.select("Country","state","Age","Gender","Timestamp")
    val df4 = df.select("self_employed","treatment","family_history","Timestamp")
    df3.createOrReplaceTempView( "left")
    df4.createOrReplaceTempView("right")


    val joinSQl = spark.sql("select left.Gender,right.treatment,left.state,right.self_employed FROM left,right where left.Timestamp = " +
      "right.Timestamp")
    joinSQl.show(numRows = 50)


    //bonus
    def parseLine(line: String) =
    {
      val fields = line.split(",")
      val Country = fields(3).toString
      val  state = fields(4).toString
      val  Gender = fields(2).toString
      (Country,state,Gender)
    }

    val lines = sc.textFile("/Users/anushamuppalla/Downloads/survey.csv")
    val rdd = lines.map(parseLine).toDF()
    rdd.show()




  }
}
