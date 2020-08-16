package tuto0

object Notes9SharedVariabelsBroadCast extends App {


  import org.apache.spark.{SparkConf, SparkContext}


    val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    /* quand on passe une fonction à une rdd qui utilise une variable déclarées à l'extérieur de cette fonction ,
    chaque exécuteur va avoir une copie de cette variable et il va appliquer cette fonction sur cette variable
     dans son coté et les résultats de chaque éxécuteur ne seront pas retournées au variable original déclarée dans
     le programme driver.Dc cette variable ne va pas etre à jour .
     pour resoudre ce problème on utilise les variables partagées càd partagées entre tout les exécuteurs
     il ya deux typpe de variable partagées:Broadcast Variables et Accumulator*/
    //1)Broadcast Variables:
    //Les variables de diffusion sont des objets partagés en lecture seule qui peuvent être créés avec la méthode
    // SparkContext.broadcastVar : et lire en utilisant value méthode value :
    val rdd = sc.parallelize(Array(2,4,6,8,10))
    //création d'une variable partagé de type broadCast
    val broadcastVar = sc.broadcast("ismahan")
    broadcastVar.value // retourne ismahan
    rdd.map(x=>x+broadcastVar.value.length)
    //spark va diffusé la variable broadcast sous une forme sérialisée dans la mémoire cache avant d'exécuté chaque tache
    //cette variable va etre desérialisé avant d'exécuter chaque tache







}
