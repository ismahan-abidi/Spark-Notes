package sparkSQL

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object Note2DataSets extends App{
  //pour créer une dataset il faut  déclarer un case class
  //un Encoder permet de convertir un objet jvm en une représentation spark sql (schema) et vis versa
  //un Encoder va etre générer dynamiquement pour le case class
  //en java il faut déclarer l'Encoder pour une classe donnée par contre spark va générer un Encoder pour le case class en scala
  val sparkss= SparkSession.builder()
    .appName("création d'une dataset")
    .master("local[2]")
    .getOrCreate()
  //voir constructeur primaire en scala https://github.com/ismahan-abidi/Scala-Notes/blob/master/src/tuto5/classes/Notes1PrimaryConstructor.scala
  case class Personne(nom : String, prenom : String, age : Long)
  val sequence = Seq(Personne("kouki","abdel",28),Personne("abidi","ismahan",28), Personne("kouki","haroun",9))
  // For implicit conversions like converting RDDs, sequence ,etc .. to DataFrames
  import sparkss.implicits._
  //dataset est fortement typée , les objets qui existe dans ds son des personnes
  val ds = sequence.toDS()
  ds.printSchema()
  ds.show()
  ds.select("nom").show()
  ds.map(x=>x.age).show()
  ds.filter(y=>y.age>9).show()
  //pour convertir une dataframe en une dataset il faut que le schema de dataframe porte des colonne qui ont les memes noms et types
  // des attributes de case classe
  val df = sparkss.read.json("files/ismahan.json")
  val ds1=df.as[Personne]
  //la différence entre RDDs et dataset est que avec  dataset on peut utiliser des requetes sql
  //pour la serialisation dataset utilise Encoder mais RDDs utilise autres serialiseurs *
  //convertir une rdd en dataset/dataframe
  //1) 1er méthode
  val rdd=sparkss.sparkContext.textFile("files/nom.txt").map(s=>Personne(s.split(",")(0),s.split(",")(1),s.split(",")(2).toInt))//rdd des personnes
  val dfrdd = rdd.toDF()
  val dsrdd = rdd.toDS()
  //2) 2eme méthode
  // étape 1: convertir rdd en rdd des row
  val rddrow = rdd.map(p=>Row(p.nom,p.prenom,p.age))
  //étape 2 : création du schéma
  val nomfield = StructField("nom",StringType,nullable = true)
  val prenomfield = StructField("prenom",StringType,nullable = true)
  val agefield = StructField("age",LongType,nullable = true)

  val arrayfields: Array[StructField]= Array(nomfield,prenomfield,agefield)

  val schema = StructType(arrayfields)

  //étape3 : appliqué le schéma sur le RDD[ROW]
 val df2 =  sparkss.createDataFrame(rddrow,schema)
  df2.printSchema()
  df2.show()










}
