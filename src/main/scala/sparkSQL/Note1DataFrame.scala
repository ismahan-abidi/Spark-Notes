package sparkSQL

import org.apache.spark.sql.SparkSession

object Note1DataFrame  extends App{
  //la méthode master prend en paramètre soit local si en est en local soit standalone ou yarn ou mesos
  //dans la jvm il faut avoir un seul objet de type sparksession si non on aura une exception c'est pour cela on utilise
  // la méthode getOrCreate() pour récupérer l'objet s'il est déja créer ou bien de le créer
  //création d'une data frame depuis un fichier csv
val sparkss= SparkSession.builder()
    .appName("création d'une data frame")
    .master("local[2]")
    .getOrCreate()

  val df = sparkss
    .read
    .option("delimiter",";")
    .option("header",true)
    .option("inferSchema","true")
   /* .format("csv")
    .load("files/correspondances-code-insee-code-postal.csv")*/ //ou bin directement en fait csv
    .csv("files/correspondances-code-insee-code-postal.csv")

  df.printSchema()
  df.show(10)

  df.select("Département","Région").show(10)
  df.createOrReplaceTempView("villes")
 sparkss.sql("select * from villes where `Code Postal`=78600").show()
  //réation d'une dataframe avec RDBMS from mysql
  val dfmysql= sparkss.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/carrefour")
  .option("driver", "com.mysql.jdbc.Driver")
  .option("dbtable", "produit")
  .option("user", "root")
  .option("password", "kouki123").load()
  dfmysql.printSchema()
  dfmysql.show(10)
  //création dataframe depuis un fichier json
  val dfjson1 = sparkss.read.json("files/example_1.json")
  val dfjson2 = sparkss.read.json("files/example_2.json")
  dfjson1.printSchema()
  dfjson2.printSchema()
  //les opérations non typées
  df.select("Code INSEE").show()
  df.groupBy("Département").count().show()
  //on va exécuter des requettes sql concrettes dans la méthode sql de l'objet spark session
  //on crée un view :c'est une table virtuelle
  //cette table est enregistrée dans l'objet spark session
  df.createOrReplaceTempView("villes")
  sparkss.sql("select count(*), commune from villes group by commune ").show()
  //Global Temporary View: dans le cas précident ou on à creé une vue temporaire cette vue la est visible que dans la session que l'a creé
  //si cette session est terminée la vue va disparaitre.
  //on peut créer une vue globale visible pour toutes les sessions à travers la méthode createGlobalTempView
  df.createGlobalTempView("monem_global")
  sparkss.sql("select * from monem_global")
  val session2 = sparkss.newSession()
  session2.sql("select * from monem_global")


}
