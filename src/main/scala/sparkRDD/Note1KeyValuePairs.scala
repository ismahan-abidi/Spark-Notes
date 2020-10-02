package sparkRDD //on scala le non du dossier pas nécessairement le nom du package

import org.apache.spark.{SparkConf, SparkContext}

object Note1KeyValuePairs extends App {
  //shullfe est un mécanisme qui permet de transférer toutes les données qui ont le meme clé dans un seul worker node
  val conf = new SparkConf().setAppName("test1").setMaster("local[1]")
  val sc = new SparkContext(conf)
  val rdd = sc.textFile("files/f1.txt")
  rdd.foreach(println)
  //pairs est une rdd des pairs clé valeur
  val rddpairs = rdd.map(s => (s, 1))
  rddpairs.foreach(println)
  //counts est une rdd des pairs clé valeur
  val counts = rddpairs.reduceByKey((a, b) => a + b)//on va appliquer la fonction en paramètre de reduceByKey sur chaque valeure
  counts.foreach(println)
  println("-----------------------------------------------------")
  val valeur =counts.sortByKey(ascending = true)
  //collect permet de retourner résultat de rdd au program driver sous forme d'un tableau
  valeur.collect().foreach(println)



}
