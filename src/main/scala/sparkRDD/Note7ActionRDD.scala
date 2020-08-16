package tuto0

import org.apache.spark.{SparkConf, SparkContext}
import tuto0.Note1KeyValuePairs.lines

object Note7ActionRDD extends App {
  val conf = new SparkConf().setAppName("test1").setMaster("local[1]")
  val sc = new SparkContext(conf)
  // l'action reduce prend en paramètre une fonction anonyme à deux paramètres et retourne un résultat autre que rdd
  println("------------------------- action reduce() ---------------------------------")
  val rdd1 = sc.textFile("files/nom.txt")
  val resultatReduce = rdd1.reduce((s1,s2) => s1+s2)
  println(resultatReduce)
  //l'action collect retourne tout les éléments de rdd sous forme d'un tableau au  program driver , cette action est généralement utile aprés
  //une transformation de filter ou une autre transformation qui permet de retourner une rdd de taille petit
  println("------------------------- action collect()  ---------------------------------")
  val actionCollect = rdd1.collect
  //println(actionCollect)//pourquoi resultat est [Ljava.lang.String;@6d1dcdff et fonctionne bien avec foreach
  actionCollect.foreach(println)
  println("------------------------- action count()   ---------------------------------")
  //l'action count retourne le nombre des éléments dans une rdd (cad le nombre de ligne)
  val actionCount = rdd1.count()
  println(actionCount)
  println("------------------------- action first()   ---------------------------------")
  //l'action first retourne le premier élément de l'rdd c'est l'équivalent de take(1)
  val actionfirst = rdd1.first()
  println(actionfirst)
  println("------------------------- action take(n)   ---------------------------------")
  //l'action take retourne les n premier élément de l'rdd
  val actionTake = rdd1.take(3)
  //on a utilisé foreach car le type de retour est un tableau des éléments
  actionTake.foreach(println)
  val seq = Array(1,2,3,4,5,6,7,8,9)
  val rddParallelize=sc.parallelize(seq)
  val take = rddParallelize.take(3)
  take.foreach(println)
  println("------------------------- action takeSample ---------------------------------")
  //l'action takeSample retourne un tableau avec un échantillon aléatoire des données
  val actionTakeSample = rdd1.takeSample(false,2)
  actionTakeSample.foreach(println)
  println("------------------------- action takeOrdered ---------------------------------")
  //l'action takeOrdered retourne les n premier éléments de l'rdd à l'aide de leur ordre naturel ou d'un comparateur personnalisé
  val actionTakeOrdered = rdd1.takeOrdered(5)
  actionTakeOrdered.foreach(println)
  println("------------------------- action saveAsTextFile ---------------------------------")
  //l'action saveAsTextFile permet de créer les éléments de l'rdd sous forme de text file
  //Spark appellera toString sur chaque élément pour le convertir en une ligne de texte dans le fichier.
  val actionSaveAsTextFile = rdd1.saveAsTextFile("files/save.txt")
  println(actionSaveAsTextFile)
  println("ok")
  println("------------------------- action saveAsSequenceFile ---------------------------------")
  //l'action saveAsSequenceFile permet de créer les éléments de l'rdd en tant que fichier de séquence Hadoop
  // dans un chemin donné dans le système de fichiers local,ne fonctionne que pour les rdd paires
  val lines = sc.textFile("files/f1.txt")
  val pairs = lines.map(s => (s, 1))
  //val actionSaveAsSequenceFile = pairs.saveAsSequenceFile("files/sequencefile")
 // println(actionSaveAsSequenceFile)
  println("------------------------- action countByKey()  ---------------------------------")
  //l'action countByKey() permet de retourner le nombre de valeurs pour chaque clé sous forme d'un hashmap(stocke les données sous forme de clé valeurs(somme))
  val rddcountByKey = pairs.countByKey()
  rddcountByKey.foreach(println)






}
