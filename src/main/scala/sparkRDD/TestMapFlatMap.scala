package sparkRDD

import org.apache.spark.{SparkConf, SparkContext}

object TestMapFlatMap extends App{
  val conf =new  SparkConf().setAppName("test").setMaster("local[2]")
  val spark = new SparkContext(conf)
  val rdd = spark.textFile("files/nom.txt")
  val rddmap = rdd.map(x => x.split(","))
  rddmap.foreach(println)
  val rddflatmap = rdd.flatMap(x => x.split(","))
  rddflatmap.foreach(println)


}
