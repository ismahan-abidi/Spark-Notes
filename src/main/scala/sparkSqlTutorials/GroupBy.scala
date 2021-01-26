package sparkSqlTutorials

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GroupBy  extends App{
  val spark = SparkSession.builder()
    .appName("Group by ")
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._
  val simpleData = Seq(("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  )
  val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
  df.show()
  // déterminer le nombre des salariés pour chaque département
  df.groupBy("department").count().show(false)
  //pour appiliquer plusieurs fonctions d'agrégations on appelle la méthode .agg()
  df.groupBy("state").agg(max("salary"),avg("age"),min("bonus")).show(false)
  //GroupBy on multiple columns
  df.groupBy("department","state")
    .sum("salary","bonus")
    .show(false)
}
