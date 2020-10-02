package tuto0

import org.apache.spark.{SparkConf, SparkContext}

object Note5PrintingElementsOfAnRDD extends App {
  val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
  val ismahen_sc = new SparkContext(conf)
  val rdd = ismahen_sc.textFile("files/transactions.txt")
  rdd.take(10).foreach(println)
  //take retourne un tableau d'élement ou chaque élement est une ligne
  //take permet de retourner un nombre bien déterminer de RDD
  //collect permet de retourner une résultat sous forme de tableau aussi tout les éléments de l' RDD
 rdd.collect().foreach(println)
}
