package tuto0

import org.apache.spark.{SparkConf, SparkContext}

object Note10SharedVariableAccumulator  extends App{
  // accumulators sont des variables partégées
  //la variable partagée contient une méthode associative et commutative qui s'appelle add() qui permet de modifier
  //la valeur de cette variable va etre modifier en parallèle par tout les taches
  //parmis les ik noussa :*
  // utilisations de cette variables sont le calcul d'une somme ou le compter (counter) qlq chose
  //on peut visualisé les valeurs d'une variable accumulateur dans une interface web
  val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val rdd = sc.textFile("files/nom.txt")
//on va calculer combien de fois le nom ismahen est répéter dans le fichier à traver une variable accumulator
  val rdd2 = rdd.flatMap(x=>x.split(","))
  //ceéation d'une variable Accumulator'
  val countIsmahanNumber = sc.longAccumulator("first variable accumulator ")
  rdd2.foreach(x=>{if (x.equals("ismahan"))
  countIsmahanNumber.add(1)
  })
  println(countIsmahanNumber.value) // retourne ismahan est répéter 4 fois
  //les tasks dans chaque worker node ne peuvent pas lire la valeur de l'accumulateur uniquemet le driver program peut le faire
  //les exécuteurs peuvent uniquement faire lèopération add càd modifier la valeur de l'accumulateur


}
