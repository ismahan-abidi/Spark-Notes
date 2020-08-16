package tuto0

import org.apache.spark.{SparkConf, SparkContext}
object Note0 extends App {
  val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
  val ismahen_sc = new SparkContext(conf) //ismahen-sc est le nom de l'objet SparkContext
  //IL YA DEUX MÉTHODES POUR CRÉER UNE rdd
  //1) Parallelized Collections
  //création d'une rdd mais il n'a pas l'éxécuter
  val data = Array(1, 2, 3, 4, 5) //data est le non de la collection
  //la méthode parallelize peut prendre soit un seul paramètre qui est le nom de la collection soit deux paramètres (nom de la collection, le nbre de partition)
  val rdd1 = ismahen_sc.parallelize(data,2)//numSlice est le nombre de partition
  rdd1.foreach(println)
  //2)Créer une rdd à travers un fichier
  val rdd2 = ismahen_sc.textFile("files/f1.txt")
  rdd2.foreach(println)
  val x= rdd2.map(s => s.length) // x est une rdd transformé
  //si on va utiliser l'RDD X une autre fois il est préfirable de la mettre dans la mémoire ou disque
  x.foreach(println)
  x.persist()
  val y = x.reduce((a, b) => a + b) // y est un entier dans cet exemple donc c'est une action
  println(y)
  //on peut ausssi creér une rdd qui permet de lire tout les fichiers d'extention .txt comme suit:
  // val rdd2 = ismahen_sc.textFile("file:///home/abidi/*.txt") ou bien *.gz
 ////// val rddfiles = ismahen_sc.wholeTextFiles("files")
 // rddfiles est une rdd des pair sous forme clé valeur car la méthode wholeTextFiles retourne un couple de RDD
  // ou le clé est le path d'un fichier et la valeur est son contenu
  //x est un couple car wholeTextFile contient un couple nom fichier , contenue fichier
 //////rddfiles.foreach(x=>println("nom fichier "+x._1+" contenue fichier "+x._2))
  //rddfiles est une rdd des  Tuple2, ou chaque Tuple contient le path de fichier et son contenu








}
