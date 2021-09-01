package sparkSqlTutorials

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object WindowingFunctions extends  App{
  val spark = SparkSession.builder().appName("windowing").master("local[2]").getOrCreate()
  import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val df = simpleData.toDF("employee_name", "department", "salary")
  df.show()
  // https://sparkbyexamples.com/spark/spark-sql-add-row-number-dataframe/
//orderBy("name column ) cad par défaut c'est ordonnée assendent
 /* Le row_number () est une fonction de fenêtre(windowing) dans Spark SQL qui attribue un numéro de ligne(nombre entier séquentiel) à chaque ligne
   du DataFrame de résultat. Cette fonction est utilisée avec Window.partitionBy() qui partitionne les données en cadres Windows et en clause orderBy ()
   pour trier les lignes de chaque partition.*/
  //row_number
  val w  = Window.partitionBy("department").orderBy("salary")
  df.withColumn("row_number",row_number.over(w))
    .show()
}
