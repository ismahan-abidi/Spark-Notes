package sparkSQL


import org.apache.spark.sql.SparkSession

object ExerciceDataFrame extends App{
  val spark= SparkSession.builder()
    .appName("creation d'une data frame")
    .master("local[2]")
    .getOrCreate()

  val df = spark
    .read
    .option("delimiter",";") //option(key,value) les key sont prédéfinies
    .option("header",true)
    .option("inferSchema","true") //inferSchema va détecter le type des colonnes
    .csv("files/correspondances-code-insee-code-postal.csv")
    .withColumnRenamed("Région","Region")

  df.printSchema()
  df.show(10)
  println("le nombre de lignes de dataframe est " +df.count())
  df.select("Code Postal","Commune","Département","Region").show(10)
  //string dans sql est entre simple cote
  df.select("Département").where("Region == 'ILE-DE-FRANCE'").distinct().show()
 // df.select("Commune").where("Population/Superficie>0").show(5)
  df.createOrReplaceTempView("villes")
  spark.sql("select Commune , Population/Superficie as densite from villes Order by densite desc ").show(5)

}
