package sparkRDD

object Notes9SharedVariabelsBroadCast extends App {
  import org.apache.spark.{SparkConf, SparkContext}

  val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    /* quand on passe une fonction à une rdd qui utilise une variable déclarées à l'extérieur de cette fonction ,
    chaque exécuteur va avoir une copie de cette variable et il va appliquer cette fonction sur cette variable
    dans son coté et les résultats de chaque éxécuteur ne seront pas retournées au variable original déclarée dans
    le program driver.Dc cette variable ne va pas etre à jour .
    pour resoudre ce problème on utilise les variables partagées càd partagées entre tous les exécuteurs
    il ya deux typpe de variable partagées:Broadcast Variables et Accumulator*/
    //1)Broadcast Variables:
    //Les variables de diffusion sont des objets partagés en lecture seule qui peuvent être créér avec la méthode
    // SparkContext.broadcastVar : et lire en utilisant la  méthode value :
 /* Quant est ce on utilise broadcast variable :
   problématique:la jointure entre deux dataframes/sets est trés couteuse en resources et en temps d'exécution(trés lente) car elle provoque l'opération shufffle
  car on doit rassembler les memes lignes qui correspondent au valeurs des clés des jointures dans le meme exécuteur afin
  de les exécuter ensemble
  solution:
  si l'une des dataframe à une petite taille alors on peut le mettre en brodcast(cad dans une variable brodcast) cad on fait une copie d'elle
  dans la memoire de chaque exécuteur pour éviter le shuffle et la jointure sera faite dans chaque exécuteur.
  attention: on ne peut pas broadcaster une dataframe/set si elle a une taille grande si non elle va planter la mémoire de l'exécuteur
   */
    val rdd = sc.parallelize(Array(2,4,6,8,10))
    //création d'une variable partagé de type broadCast
    val broadcastVar = sc.broadcast(List(1,2,3))
    println(broadcastVar.value) // retourne la List(1,2,3) la variable broadcast c'est just pour la lecture
    val rdd2 = rdd.map(x=>x+broadcastVar.value.length)
  rdd2.foreach(println)
    //spark va diffusé la variable broadcast sous une forme sérialisée dans la mémoire cache avant d'exécuter chaque tache
    //cette variable va etre desérialisé avant d'exécuter chaque tache

}
