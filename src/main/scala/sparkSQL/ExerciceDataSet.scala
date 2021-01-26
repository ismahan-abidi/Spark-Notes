package sparkSQL

import org.apache.spark.sql.SparkSession

object ExerciceDataSet extends App{

  val spark= SparkSession.builder()
    .appName("création d'une dataset exercice ")
    .master("local[2]")
    .getOrCreate()

  val df =spark
    .read
    .option("delimiter",";") //option(key,value) les key sont prédéfinies
    .option("header",true)
    .option("inferSchema","true") //inferSchema va détecter le type des colonnes
    .csv("files/correspondances-code-insee-code-postal.csv")
    .withColumnRenamed("Code INSEE","CodeINSEE")
    .withColumnRenamed( "Code Postal","CodePostal")
    .withColumnRenamed("Région","Region")
    .withColumnRenamed("Altitude Moyenne","AltitudeMoyenne")
    .withColumnRenamed("ID Geofla","IDGeofla")
    .withColumnRenamed("Code Commune","CodeCommune")
    .withColumnRenamed("Code Canton","CodeCanton")
    .withColumnRenamed("Code Arrondissement","CodeArrondissement")
    .withColumnRenamed("Code Département","CodeDépartement")
    .withColumnRenamed("Code Région","CodeRégion")
  df.printSchema()
case class CodeVille(CodeINSEE :Int, CodePostal : String, Commune : String, Département : String , Region : String, Statut:String, AltitudeMoyenne: Double,
                     Superficie:Double,Population:Double, geo_point_2d: String , geo_shape:String,IDGeofla:Int, CodeCommune:Int,CodeCanton:Int,
                     CodeArrondissement:Int, CodeDépartement:Int,CodeRégion:Int )
  import spark.implicits._ //obligatoire car nous avons covertir une dataframe en dataset
  val ds =df.as[CodeVille]
  ds.printSchema()
  println("le nombre de ligne de dtaset est "+ds.count())
  ds.select("CodePostal","Commune","Département","Region").show(10)
  //where c'est une méthode de type dataset[T]
  ds.select("Département").where("Region = 'ILE-DE-FRANCE'").distinct().show()
  ds.createOrReplaceTempView("Villes")
  spark.sql("select commune , population/superficie as densite from villes order by densite desc").show(5)


}
