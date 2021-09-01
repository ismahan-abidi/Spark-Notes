package sparkSqlTutorials

import org.apache.spark.sql.{DataFrame, SparkSession}

object ExerciceDataFrameLansrod extends  App{
  implicit val sparkss= SparkSession.builder()
    .appName("ExerciceDataFrame")
    .master("local[2]")
    .getOrCreate()

  def loadDf(path: String)(implicit spark :SparkSession): DataFrame = {
    val df = spark
      .read
      .option("delimiter",";")
      .option("header",true)
      .option("inferSchema","true")
      .csv(path)
  df
  }
 val contractDF=  loadDf("/home/abidi/IdeaProjects/SparkNotes/src/main/scala/sparkSqlTutorials/contrat.csv")
  contractDF.show()
  val gravityDF = loadDf("/home/abidi/IdeaProjects/SparkNotes/src/main/scala/sparkSqlTutorials/gravity.csv")
  gravityDF.show()
//une fonction ne sera exécuter qu'aprés appel dans le main
  def calculGravityForContract(contractDf: DataFrame, gravityDF: DataFrame) : DataFrame = {
    val minContractGravityDF = gravityDF.groupBy("pole_id" ,"appli_emet").min("contract_gravity")
      .withColumnRenamed("pole_id" , "pole_id_gravity")
      .withColumnRenamed("appli_emet" , "appli_emet_gravity")
      .withColumnRenamed("min(contract_gravity)" , "contract_gravity")
     contractDf.join(minContractGravityDF,minContractGravityDF("pole_id_gravity") === contractDf("pole_id") && minContractGravityDF("appli_emet_gravity")  === contractDf("appli_emet") , "left")
       .select("contract_id","pole_id","appli_emet","montant","contract_gravity")

  }
  val  df1 = calculGravityForContract(contractDF,gravityDF)
  df1.show()


}
