package sparkSQL

object DS_vs_DF {
/*  /////////////////////////////////////////////
  /////// DataFrame VS DataSet ///////////////
  ///////////////////////////////////////////
  * Depuis la version 2.0 de spark, les API  DataFrame & DataSet sont unifiées, DataFrame est juste
  un alias sur DataSet[Row], dans les versions précédentes ils étaient deux class différentes.

    * DataSet c'est une collection des données immutable distribuée, elle fournit les avantages des RDD,comme
  le typage fort et les expressions lambda,
  - DataSet est type safe
  - Python ne supporte pas DataSet, car Python est une langage de programmation dont les types des variables
    sont évaluées au moment d'exécution et non au moment de compilation
    - Encoder : c'est un class qui permet de convertir un objet JVM en une répresentation Spark SQL
    (le schema SQL) et vis-versa
  Pour java il faut écrire éxplecitement l'Encoder dans le code
  Pour scala, c'est généré automatiquement pour les case class
----------------------------------------------------------------------------------------------------
  * DataFrame est une collection des données immutable distribuée dont les données sont orgaisées dans
  des colonnes comme dans une table d'une base des données relationelle grace à un schema imposé.
  - Dataframe est non typée, car DataFrame est un alias de DataSet[Row] avec Row est un objet JVM non typé
  - Dataframe n'est pas type safe, c'à'd elle ne va pas détecter une colonne qui n'existe pas  ou bien
    un type erronné d'une colonne existante au moment de compilation, mais plutot au moment d'exécution.*/

}
