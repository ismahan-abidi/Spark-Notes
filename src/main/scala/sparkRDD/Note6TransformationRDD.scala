package sparkRDD

import org.apache.spark.{SparkConf, SparkContext}

object Note6TransformationRDD extends App {
  val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
  val ismahen_sc = new SparkContext(conf)
  val rdd1 = ismahen_sc.textFile("files/nom.txt")
  //la fonction qu'on passe pour La méthode flatMap  permet de retourner  pour chaque élement 0 ou plusieurs éléments
  println(" -----------la méthode flatMap -------------------")
 val rddFlatMap= rdd1.flatMap(x => x.split(","))
  rddFlatMap.foreach(println)
  //la méthode Sample permet de créer  une rdd echantillon
  println(" -----------la méthode sample -------------------")
 val rddSample = rddFlatMap.sample(false, 0.5)
  rddSample.foreach(println)
  rddSample.foreach(println)
  //la méthode union permet de retourner l'union des données dans les deux rdd
  println(" -----------la méthode union -------------------")
  val rddUnion = rdd1.union(rddFlatMap)
  rddUnion.foreach(println)
  //la méthode intersection Renvoie un nouveau RDD qui contient l'intersection des éléments
  println(" -----------la méthode intersection -------------------")
  val rddIntersection = rdd1.intersection(rddFlatMap)
  rddIntersection.foreach(println)
  //la méthode distinct permet de retourner les élements qui sont ditinctes cad qui ne se repètent pas
  println(" -----------la méthode distinct -------------------")
  val rddDistinct = rdd1.distinct()
  rddDistinct.foreach(println)
  //la méthode groupByKey permet de grouper les éléments de rdd clé_valeur (rdd de tuple2) sous forme de groupe clé valeur
  println(" -----------la méthode groupByKey -------------------")
  val pairs = rdd1.map(s => (s, 1))
  val rddGroupByKey = pairs.groupByKey()
  rddGroupByKey.foreach(println)
  //la méthode reduceByKey permet de regrouper les entiers sur la base de la même clé et de faire ses sommes
  println(" -----------la méthode reduceByKey -------------------")
  val rddReduceByKey = pairs.reduceByKey((a,b)=>a+b)
  rddReduceByKey.foreach(println)
  //cette transformation aggregateByKey permet de créer une pair rdd ou le type de valeur de rdd résultat est différent au type de
 //valeur de rdd initiale elle prend 3 paramètres
 // 1) ZeroValue: contient la valeur initiale du premier paramètre de la fonction SeqOp
 // 2) SeqOp est une fonction qui va etre appliquer sur chaque valeur de chaque partition pour créer une valeur de meme type de rdd resultat
 // 3) CombOp c'est une fonction qui va etre appliquer sur les résultats de SeqOp pour agréger les nouvalle valeurs
 println(" -----------la méthode aggregateByKey -------------------")
 val seqOp = (x : String,y:Int) => if(y>2) x+" abidi " else "x ############ "
 val combOp =(x:String, y :String) => x+y
 val rddaggregateByKey = rddReduceByKey.aggregateByKey("")(seqOp,combOp)
 rddaggregateByKey.foreach(println)
 println(" -----------la méthode sortedByKey -------------------")
 // la transformation sortedByKey pour chaque partition  permet de triéer par clés dans l'ordre croissant ou décroissant,
 val rddSortedByKey = rddReduceByKey.sortByKey(true,1)
 rddSortedByKey.foreach(println)
 println(" -----------la méthode join -------------------")
 


}
