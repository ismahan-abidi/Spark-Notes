package sparkSQL

import org.apache.spark.sql.SparkSession


object Notes5SavingToHive extends App{
  //pour travailler spark avec hive il faut appeler la méthode enableHiveSupport
  // pour permettre spark à lire et écrire depuis le metastore de hive
  //spark n'a pas besoin que hive soit installé si on veut tester les traitements sur hive en locale car spark
  //va créer un métastore(base de donnée relationnelle) de type DERBY en local et il va stocker les données
  //dans le système de fichier local
  //si on a un erreur de ce type  Unable to instantiate SparkSession with Hive support because Hive classes are not found. il suffit
  //d'enlever les dépendances qui cause les conflicts(dans intellij il suffit d'installer le plugin maven helper pour supprimer les dépendances des conflicts.
  //maven helper est un plugin qui permet de voir l'arbre des dépandences du projets et de fixer les conflicts
  //plugin(brancher) est un logiciel qui permet d'ajouter des fonctionalités à un autre logiciel (intellij)
  //on trouve l'arbre des dépendances dans un tab de fichier pom.xml en bas de la page qui s'appelle Dependency Analyzer
  val sparkss=SparkSession.builder()
    .enableHiveSupport()
    .appName("création d'une data frame")
    .master("local[2]")
    .config("spark.sql.warehouse.dir", "/home/abidi/IdeaProjects/SparkNotes/files/hive") //permettre à spark de savoir où il va stocker les fichiers de hive
    .getOrCreate()

 val df = sparkss.read.json("files/ismahan.json")
  sparkss.sql("drop table if exists table1")
  df.write.saveAsTable("table1")
  //on peut créer une dataframe à partir d'une table existante dans hive à partir de la méthode table()
  val df1=sparkss.table("table1")
  df1.printSchema()
  df1.show()
  sparkss.sql("create database if not exists sparkwithHive")
 //création d'une table partitionnée
  val dfcsv = sparkss.read.json("files/ismahan.json")
 dfcsv.write.partitionBy("ville").saveAsTable("sparkwithHive.partitionnedtable1")




}
