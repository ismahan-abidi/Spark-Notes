package sparkSqlTutorials

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Jointures extends  App{
  val spark = SparkSession.builder()
    .appName("creation d'une data frame")
    .master("local[2]")
    .getOrCreate()

  val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
    (6,"Brown",2,"2010","50","",-1)
  )
  val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
    "emp_dept_id","gender","salary")
  import spark.sqlContext.implicits._
  val empDF = emp.toDF(empColumns:_*)
  empDF.show(false)

  val dept = Seq(("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
  )
  val deptColumns = Seq("dept_name","dept_id")
  val deptDF = dept.toDF(deptColumns:_*)
  deptDF.show(false)

  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"inner")
    .show(false)

 val df1 = empDF.join(deptDF,deptDF("dept_id") === empDF("emp_dept_id"),"left")
  df1.show()
//on a affiché les salariers dont leurs département est de nom finance:on a utiliser jointure, filtre et select(pour choisir les colonnes consernées)
  empDF.join(deptDF,empDF("emp_dept_id") === deptDF("dept_id") , "left").filter($"dept_name" === "Finance").select(col("name") , col("dept_name")).show()

  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"right")
    .show(false)
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"rightouter")
    .show(false)
}
