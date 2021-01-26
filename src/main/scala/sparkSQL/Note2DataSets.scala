package sparkSQL

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object Note2DataSets extends App {
  //pour créer une dataset il faut  déclarer une case class
  //un Encoder permet de convertir un objet jvm en une représentation spark sql (schema) et vis versa
  //un Encoder va etre générer dynamiquement pour le case class
  //en java il faut déclarer l'Encoder pour une classe donnée par contre spark va générer un Encoder pour le case class en scala
  val sparkss = SparkSession.builder()
    .appName("création d'une dataset")
    .master("local[2]")
    .getOrCreate()
  //voir constructeur primaire en scala https://github.com/ismahan-abidi/Scala-Notes/blob/master/src/tuto5/classes/Notes1PrimaryConstructor.scala
  case class Personne(nom: String, prenom: String, age: Long)

  val sequence = Seq(Personne("kouki", "abdel", 28), Personne("abidi", "ismahan", 28), Personne("kouki", "haroun", 1)) //on a utilisé séquence just pour tester car seq ne peut pas étre de grand taille
  // For implicit conversions like converting RDDs, sequence ,etc .. to DataFrames/DataSets
  import sparkss.implicits._
  //dataset est fortement typée , les objets qui existent dans dataset sont des personnes
  val ds = sequence.toDS()
  ds.printSchema()
  ds.show()
  ds.select("nom").show()
  ds.map(x => x.age).show()
  ds.filter(y => y.age > 1).show()

  //pour convertir une dataframe en une dataset il faut que le schema de dataframe porte des colonnes qui ont les memes noms et types
  // des attributes de case classe
  //la différence entre RDDs et dataset est que avec  dataset on peut utiliser des requetes sql
  //pour la serialisation dataset utilise Encoder mais RDDs utilise autres serialiseurs *
  //convertir une rdd en dataset/dataframe
  //1) 1er méthode
  def creerPersonne(ligne: String): Personne = {
    val elements = ligne.split(",")
    val nom = elements(0)
    val prenom = elements(1)
    val age = elements(2).toInt
    return new Personne(nom, prenom, age) //création d'un objet personne , en case classe pour créer un objet new n'est pas nécessaire et return
  }

  val rdd = sparkss.sparkContext.textFile("files/nom.txt")
  val rddPersonne = rdd.map(s => creerPersonne(s))
  //val rdd=sparkss.sparkContext.textFile("files/nom.txt").map(s=>Personne(s.split(",")(0),s.split(",")(1),s.split(",")(2).toInt))//rdd des personnes
  val dfrdd = rddPersonne.toDF()
  dfrdd.printSchema()
  val dsrdd = rddPersonne.toDS()
  dsrdd.printSchema()
  //2) 2eme méthode
  // étape 1: convertir rdd en rdd des row
  val rddrow = rddPersonne.map(p => Row(p.nom, p.prenom, p.age))
  //étape 2 : création du schéma de dataframe
  val nomfield = StructField("nom", StringType, nullable = true)
  val prenomfield = StructField("prenom", StringType, nullable = true)
  val agefield = StructField("age", LongType, nullable = true)
  val arrayfields: Array[StructField] = Array(nomfield, prenomfield, agefield)
  val schema = StructType(arrayfields)
  //étape3 : appliqué le schéma sur le RDD[ROW]
  val df2 = sparkss.createDataFrame(rddrow, schema)
  println("**** CRÉATION D'UNE DATAFRAME PAR LA MÉTHODE DE RDD[ROW] ****")
  df2.printSchema()
  df2.show()
  val ds2 = df2.as[Personne]
  ds2.printSchema()
  ds2.show()

}
