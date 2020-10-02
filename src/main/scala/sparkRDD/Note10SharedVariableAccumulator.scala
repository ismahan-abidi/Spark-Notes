package tuto0

import org.apache.spark.{SparkConf, SparkContext}

object Note10SharedVariableAccumulator  extends App{
  // accumulators sont des variables partagées
  //la variable partagée contient une méthode associative et commutative qui s'appelle add() qui permet de modifier
  //la valeur de cette variable dans chaque exécuteur.
  // Chaque exécuteur va retourner au driver le résultat calculer et le driver va faire la somme
  //parmi les cas d'utilisations de l'Accumulator est le calcul d'une somme ou de compter (counter) qlq chose
  //on peut visualisé les valeurs d'une variable accumulateur dans une interface web
  val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val rdd = sc.textFile("files/nom.txt")
//on va calculer combien de fois le nom ismahan est répété dans le fichier à travers une variable accumulator
  val rdd2 = rdd.flatMap(x=>x.split(","))
  //ceéation d'une variable Accumulator'
  val countIsmahanNumber = sc.longAccumulator("first variable accumulator ")
  rdd2.foreach(x=>{if (x.equals("ismahan"))
  countIsmahanNumber.add(1)
  })
  println("ismahan est répété "+ countIsmahanNumber.value +" fois") // retourne ismahan est répéter 4 fois
  //les tasks dans chaque worker node ne peuvent pas lire la valeur de l'accumulateur. uniquemet le driver program peut le faire
  //les exécuteurs peuvent uniquement faire l'opération add càd modifier la valeur de l'accumulateur


}
