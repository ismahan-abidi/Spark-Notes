package tuto0

import org.apache.spark.{SparkConf, SparkContext}

object  Note3Closures extends App {
  val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
  val sc = new SparkContext(conf)
  //closures est une fonction qui depend d'une variable qui n'est pas dans son scope(champs de visibilité) càd une
  //variable qui existe en dehors de cette fonction
  var counter = 0
  var data = List(0,1,2,3)
  var rdd = sc.parallelize(data)

  // Wrong: Don't do this!!
  //x => counter += x :::est une fonction anonyme dans la méthode foreach qui utilise la variable counter qui n'est pas été déclaré dans cette fonction
  //cette varable va etre copieé dans tous les exécuteurs et chaque exécuteur va traité cette variable et la variable principale dans le driver va rester à 0
  rdd.foreach(x => counter += x)

  println("Counter value: " + counter)


}
