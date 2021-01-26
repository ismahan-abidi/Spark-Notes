package sparkRDD //on scala le non du dossier pas nécessairement le nom du package

import org.apache.spark.{SparkConf, SparkContext}

object Note1KeyValuePairs extends App {
  //shullfe est un mécanisme qui permet de transférer toutes les données qui ont le meme clé dans un seul worker node
  val conf = new SparkConf().setAppName("test1").setMaster("local[1]")
  val sc = new SparkContext(conf)
  val rdd = sc.textFile("files/f1.txt")
  rdd.foreach(println)
  //rddpairs est une rdd des pairs clé valeur
  val rddpairs = rdd.map(s => (s, 1))
  rddpairs.foreach(println)
  //counts est une rdd des pairs clé valeur
  val counts = rddpairs.reduceByKey((a, b) => a + b)//on va appliquer la fonction en paramètre de reduceByKey sur chaque valeur
  counts.foreach(println)
  println("-----------------------------------------------------")
  val valeur =counts.sortByKey(ascending = true)
  //collect permet de retourner résultat de rdd au program driver sous forme d'un tableau c'est déconseillé de l'utiliser car driver est une seule machine
  // et en cas de donneés volumineuses le driver ne suffit pas
  valeur.collect().foreach(println)



}
