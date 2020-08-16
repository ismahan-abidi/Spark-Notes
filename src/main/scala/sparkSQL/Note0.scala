package sparkSQL

import org.apache.spark.sql.SparkSession

object Note0 extends App {
  //création d'un objet de type spark session qui est le point d'entrée pour faire des fonctionalités sql
  val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[4]").getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames

  import spark.implicits._

  val dataFrame = spark.read.json("files/ismahan.json")
  dataFrame.printSchema()
  dataFrame.show()
  dataFrame.select("nom").show()
  //on peut faire un select sur un ou plusieurs champs , le type de retour de select est un data frame
  dataFrame.filter($"age" > 20).show()
  //pour appliquer groupBy il faut appliquer une fonction d'agregation
  dataFrame.groupBy("prenom").count().show()
  //Running SQL Queries Programmatically
  // Register the DataFrame as a SQL temporary view
  //view c'est une table virtuelle basée sur une requette sql
  //people est un view
  dataFrame.createOrReplaceTempView("name_view")//cette view n'est pas partagé entre les autres application et sa durée de vie est liée à l'application
  val sqlDF = spark.sql("SELECT * FROM name_view")
  sqlDF.show()
  //pour créer un vue partagé entre toute les application il suffit de créer un global temporary view
  //ce vue existe toujours sous la base de données global_temp
  // Register the DataFrame as a global temporary view
  dataFrame.createGlobalTempView("ismahan_view")
  // Global temporary view is tied to a system preserved database `global_temp`
  spark.sql("SELECT * FROM global_temp.ismahan_view").show()

}
