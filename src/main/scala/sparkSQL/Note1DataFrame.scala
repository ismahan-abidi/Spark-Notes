package sparkSQL
import org.apache.spark.sql.SparkSession

object Note1DataFrame  extends App{
  //la méthode master prend en paramètre soit local si en est en local soit standalone ou yarn ou mesos
  //dans la jvm on peut  avoir plusieurs  objets de type sparksession
  //création d'une data frame depuis un fichier csv
val sparkss= SparkSession.builder()
    .appName("création d'une data frame")
    .master("local[2]")
    .getOrCreate()

  val df = sparkss
    .read
    .option("delimiter",";") // ou sep au lieu de delimiter càd séparateur
    .option("header",true)
    .option("inferSchema","true") //inferSchema va détecter le type des colonnes
   /* .format("csv")
    .load("files/correspondances-code-insee-code-postal.csv")*/ //ou bien directement en fait csv
    .csv("files/correspondances-code-insee-code-postal.csv")

  df.printSchema()
  df.show(10)
  df.select("Département","Région").show(10)
  df.createOrReplaceTempView("villes")
  //pour utiliser la méthode sql on utilise soit  tempView (local ou global) soit dans hive (car on utilise la méthode enableHiveSupport())
 sparkss.sql("select * from villes where `Code Postal`=78600").show()
  //création d'une dataframe (avec RDBMS from mysql) à partir d'une table d'une base de données relationnelle
  val dfmysql= sparkss.read.format("jdbc")
    //pour connecter sur une base de donnée il faut ajouter ces dependences maven
    //il faut que mysql soit ouvert
    //on tape sur google mysql jdbc url pour touver jdbc:mysql://localhost:3306/name-database
    //jdbc:mysql://localhost:3306/carrefour::jdbc:nom base de donnée soit mysql soit postgresql,..://adresseip de serveur ou on a téléchargé mysql:3306 port par defaut mysql
  .option("url", "jdbc:mysql://localhost:3306/carrefour")//carrefour est une base de donnée enregistré dans mysql
  .option("driver", "com.mysql.jdbc.Driver")
  .option("dbtable", "produit")
  .option("user", "root")  //user de mysql par défaut s'appelle root
  .option("password", "kouki123").load()
  dfmysql.printSchema()
  dfmysql.show(10)
  //création dataframe depuis un fichier json
  val dfjson1 = sparkss.read.json("files/example_1.json")
  val dfjson2 = sparkss.read.json("files/example_2.json")
  dfjson1.printSchema()
  dfjson2.printSchema()
  //les opérations non typées
  df.select("Code INSEE").show() // show() affiche par defaut que 20 lignes
  df.groupBy("Département").count().show()
  //on va exécuter des requettes sql concrettes dans la méthode sql de l'objet spark session
  //on crée un view :c'est une table virtuelle
  //cette table est enregistrée dans l'objet spark session
  df.createOrReplaceTempView("villes")
  sparkss.sql("select count(*), commune from villes group by commune ").show(30)
  //Global Temporary View: dans le cas précident ou on a creé une vue temporaire cette vue la est visible que dans la session que l'a creé
  //si cette session est terminée la vue va disparaitre.
  //on peut créer une vue globale visible pour toutes les sessions à travers la méthode createGlobalTempView
  df.createGlobalTempView("monem_global")
  sparkss.sql("select * from global_temp.monem_global").show(10)
  //sparkss.stop()
  val sparkss2 = sparkss.newSession()//création d'un objet sparksession d'un autre objet sparksession pour tester global view
  sparkss2.sql("select * from global_temp.monem_global").show(4)
  sparkss2.sql("select * from villes").show() //exception car villes est TempView pas GlobalView


}
