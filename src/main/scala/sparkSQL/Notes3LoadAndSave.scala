package sparkSQL

import org.apache.spark.sql.functions.{concat,lit}
import org.apache.spark.sql.SparkSession

object Notes3LoadAndSave extends App{
  val sparkss= SparkSession.builder()
    .appName("load and save")
    .master("local[2]")
    .getOrCreate()
//par défaut si on ne spécifie pas le format de fichier source spark va considérer la source sous format parquet
  val df = sparkss.read.load("files/users.parquet")
  df.printSchema()
  df.show()
 // df.select("name","favorite_color").write.save("files/result_sous_users_parquet") on a importer les fonctions lit et concat depuis org.apache.spark.sql.functions
  df.select(concat(df("name"),lit(" ismahan")),df("favorite_color")).show(10)
  //on peut requeter un fichier directement au lieu de le charger dans une dataframe
  val sqlDF = sparkss.sql("SELECT * FROM parquet.`files/users.parquet`")
  sqlDF.show()
  val jsonDF = sparkss.sql("SELECT * FROM json.`files/ismahan.json`")
  jsonDF.show()

}
