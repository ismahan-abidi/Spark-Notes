package sparkRDD //on scala le non du dossier pas nécessairement le nom du package

import org.apache.spark.{SparkConf, SparkContext}

object Note1KeyValuePairs extends App {
  //shullfe est un mécanisme qui permet de transférer toutes les données qui ont le meme clé dans un seul worker node
  val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("files/f1.txt")
  lines.foreach(println)
  //pairs est une rdd des pairs clé valeur
  val pairs = lines.map(s => (s, 1))
  pairs.foreach(println)
  //counts est une rdd des pairs clé valeur
  val counts = pairs.reduceByKey((a, b) => a + b)
  counts.foreach(println)
  println("-----------------------------------------------------")
  val valeur =counts.sortByKey(ascending = true)
  //collect permet de retourner résultat de rdd au program driver sous forme d'un tableau
  valeur.collect().foreach(println)



}
