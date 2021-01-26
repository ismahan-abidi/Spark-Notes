package sparkSqlTutorials

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CreateDataFrameFromRDD extends App {
  val spark = SparkSession.builder()
    .appName("creation d'une data frame")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val columns = Seq("language", "users_count")
  val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
  //Spark Create DataFrame from RDD
  val rdd = spark.sparkContext.parallelize(data)
  //1) Using toDF() function
  val dfFromRDD = rdd.toDF()
  dfFromRDD.printSchema()
  //pour donner des nom au columns
  val dfFromRDD1 = rdd.toDF("language", "users_count")
  dfFromRDD1.printSchema()
  //2) Using Spark createDataFrame() from SparkSession
  val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns: _*)
  dfFromRDD2.printSchema()
  dfFromRDD2.show()
  //3) Using createDataFrame() with the Row type
  //Row est un objet jvm non typé
  val schema = StructType(Array(StructField("language", StringType, true), StructField("users_count", IntegerType, true)))
  val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
  val dfFromRDD3 = spark.createDataFrame(rowRDD, schema)
  dfFromRDD3.printSchema()
  dfFromRDD3.show()
  //Create Spark DataFrame from List and Seq Collection
  //1) Using toDF() on List or Seq collection
  val dfFromData1 = data.toDF()
  dfFromData1.printSchema()
  dfFromData1.show()
  //2) Using createDataFrame() from SparkSession
  var dfFromData2 = spark.createDataFrame(data).toDF(columns:_*)
  dfFromData2.printSchema()
  dfFromData2.show()
  //3) Using createDataFrame() with the Row type
  //From Data (USING createDataFrame and Adding schema using StructType)

  val seqRow = Seq(Row("Java", 20000),
    Row("Python", 100000),
    Row("Scala", 3000))
  //createDataFrame prend en paramètre (rddRow , schema) donc il faut convertir la seq de row seqRow en rddRow
  val rddRowData = spark.sparkContext.parallelize(seqRow)
  var dfFromData3 = spark.createDataFrame(rddRowData,schema)
  dfFromData3.printSchema()
  dfFromData3.show()

}
