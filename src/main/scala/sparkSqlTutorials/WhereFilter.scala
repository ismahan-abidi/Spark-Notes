package sparkSqlTutorials

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.functions._

object WhereFilter extends App{
  val spark = SparkSession.builder()
    .appName("filter")
    .master("local[2]")
    .getOrCreate()
  //Spark DataFrame filter() Syntaxes
  val arrayStructureData = Seq(
    Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
    Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
    Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
    Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
    Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
    Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
  )

  val arrayStructureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("languages", ArrayType(StringType))
    .add("state", StringType)
    .add("gender", StringType)

  val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData) , arrayStructureSchema)
  df.printSchema()
  df.show(false)
  //création d'un filtre avec les fonctions du package org.apache.spark.sql.functions._ (col(), lit(), ...)
  import spark.implicits._
  // pour comparer deux colonnes ou deux valeurs  en scala par ===
  //avec les conditions en scala on utilise $
  val df1 = df.filter($"state" === "NY" )
  df1.show(false)
  val df11 = df.filter(col("gender") === "F")
  df11.show()
  val df3 = df.filter(df("state") === "OH")
  df3.show(false)
  val df4 = df.filter(df("state") === "NY" && df("gender") === "F")
  df4.show(false)
//la condition est une expression scala
  val df5 = df.filter("state = 'OH'")
  df5.show(false)
val df6 = df.filter($"name.lastname" === "Williams" )
  df6.show(false)
  //la fonction array_contains() Renvoie null si le tableau est nul, vrai si le tableau contient une valeur et faux dans le cas contraire.
  val df7 = df.filter(array_contains(col("languages"),"Java"))
  df7.show(false)
  //les deux fonctions filter() et where() sont exactement la meme chose
  val df8 = df.where(array_contains(col("languages"),"Java"))
  df8.show(false)


}
