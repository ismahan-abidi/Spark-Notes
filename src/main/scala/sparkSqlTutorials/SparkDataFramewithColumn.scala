package sparkSqlTutorials

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions._  //pour importer des fonctions comme lit(), col(), concat() etc...

object SparkDataFramewithColumn  extends App{
  //La fonction Spark withColumn() est utilisée pour renommer, changer la valeur, convertir le type de données
  // d'une colonne DataFrame existante et peut également être utilisée pour créer une nouvelle colonne

  val spark = SparkSession.builder()
    .appName("creation d'une data frame")
    .master("local[2]")
    .getOrCreate()

  val data = Seq(Row(Row("James ","","Smith"),"36636","M","3000"),
    Row(Row("Michael ","Rose",""),"40288","M","4000"),
    Row(Row("Robert ","","Williams"),"42114","M","4000"),
    Row(Row("Maria ","Anne","Jones"),"39192","F","4000"),
    Row(Row("Jen","Mary","Brown"),"","F","-1")
  )
  val schema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType,true)
      .add("middlename",StringType,true)
      .add("lastname",StringType,true))
    .add("dob",StringType,true)
    .add("gender",StringType,true)
    .add("salary",StringType,true)
  val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
  //1. Spark withColumn – To change column DataType
  val df1 = df.withColumn("salary",col("salary").cast("Integer"))
  df1.printSchema()
  //2. Change the value of an existing column
 val df2= df1.withColumn("salary",col("salary")*100)
  df2.show(false)
  //3. Derive new column from an existing column
  // si on change le nom de la colonne dans le premier paramétre de la méthode withColumn, alors une autre colonne serait créer avec le nouveau nom
  val df3=df1.withColumn("salary2",col("salary") - 50)
  df3.show(false)
  //4. Add a new column
  val df4 = df3.withColumn("age",lit(20)).withColumn("origin",lit("TUNISIA"))
  df4.show(false)
  //5. Rename DataFrame column name
  val df5 = df4.withColumnRenamed("dob" , "dateOfBirth")
df5.show(false)
  //6. drop column
  val df6 = df5.drop("salary2")
  df6.show(false)
  // concatiner deux colonne
  // lit permet de créer une valeur constante
  val df7 = df6.withColumn("test_concat",concat(col("gender"),lit(" ### "),col("origin")))
  df7.show(false)


}
