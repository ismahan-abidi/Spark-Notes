package sparkSQL

import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Notes4SaveMode extends App{
  val sparkss= SparkSession.builder()
    .appName("création d'une data frame et save mode")
    .master("local[2]")
    .getOrCreate()
  val df = sparkss.read.load("files/users.parquet")
  //pour gérer les données existantes on doit  préciser  SaveMode dans la méthode mode
  //il ya 4 modes d'enregistrement de dataframe/dataset
  //1)si on veut écraser les données existantes on utilise Overwrite
  df.select("name","favorite_color").write.mode(SaveMode.Overwrite).save("files/result_sous_users_parquet")
  df.select("name","favorite_color").write.mode(SaveMode.Overwrite).save("files/result_sous_users_parquet")
  //2)si on veut ajouter les nouvelles données(fichiers) au encients fichiers existe déja  au données existantes on utilise Append
  val df1 = df.select(concat(df("name"),lit(" ismahan")),df("favorite_color")).withColumnRenamed("concat(name,  ismahan)", "name")
  df1.printSchema()
 df1.write.mode(SaveMode.Append).save("files/result_sous_users_parquet")
  //3)Ignore:c'est le meme principe du clause create table if not exists en sql c'à d si les données existe sspark va rien faire
  df1.write.mode(SaveMode.Ignore).save("files/result_sous_users_parquet3")
  df1.write.mode(SaveMode.Ignore).save("files/result_sous_users_parquet3")
  //4)ErrorIfExists:c'est le mode d'enregistrement par defaut si on ne le précise pas
  df1.write.mode(SaveMode.ErrorIfExists).save("files/result_sous_users_parquet3")



}
