package tuto0

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object Note8RDDpersistence extends App{
  val conf = new SparkConf().setAppName("test1").setMaster("local[2]")
  val ismahen_sc = new SparkContext(conf) //ismahen-sc est le nom du SparkContext
  val rdd2 = ismahen_sc.textFile("files/f1.txt")
  //Puisque Les RDD créer à travers les transformations sont lazy càd les traitements dans la trasformation ne seraient faits
  //que si une action est appliqueé sur cette rdd , donc si on va appliquer une autre action sur cette rdd le traitement de
  //cette transformation va etre récalculer de nouveau entièrement .
  //ca sera moins efficace si on va appliqué plusieurs actions sur la meme RDD donc il faut le persister pour éviter
  // la répétation de traitement à chaque fois.
  // Il ya deux méthodes pour persister une RDD dans la mémoire ou dans le disque
  //la première: cache() c'est pour persister une RDD dans la mémoire c'est la méthode la plus recommendée si le cluster à assez de mémoire libre
  //car traiter les données dans la mémoire est 10 fois plus rapide que le traitement dans le disque
  rdd2.cache()
  //deuxième méthode: persist(storage level) prend en paramètre un objet de type storage level qui détermine en quoi on va persister la RDD
  //LE STORAGE LEVEL PEUT PRENDRE :
  //1)MEMORY_ONLY ; c'est exactement equivalent à la méthode cach() car il traite toute les partitions de la RDD dans la mémoire
  //les objets de l'RDD sont décérialisé dans la JVM
  rdd2.persist(StorageLevel.MEMORY_ONLY)
  //2) MEMORY_AND_DISK :dans un premier temps les partitions des rdd seront persistées dans la mémoire et les objets sont déséréalisés
  //et si la mémoire ne suffit pas les partitions qui ne trouve pas de mémoire seront persistées dan le disque
  rdd2.persist(StorageLevel.MEMORY_AND_DISK)
  //3)MEMORY_ONLY_SER: pareille à mémory only mais les objets seront serialisées dans la JVM
  //ca peut nous gagnier de la mémoire mais c'est couteux pour le CPU pendant la lecture car il doit décéréaliser les objets
  rdd2.persist(StorageLevel.MEMORY_ONLY_SER)
  //4)MEMORY_AND_DISK_SER: pareille à memory_only_ser mais si la mémoire ne suffit pas pour certaines partitions alors elles
  //seront persistées dan le disque.
  rdd2.persist(StorageLevel.MEMORY_AND_DISK_SER)
  //5)DISK_ONLY  : càd persiste toute l'rdd dans le disque seulement
  rdd2.persist(StorageLevel.DISK_ONLY)
  //6)MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc. pareille pour tout ce qu'on a vu mais chaque partition sera dupliqué dans deux noeuds
  //pour etre trés rapide dans le cas d'une  panne c'à d pour éviter de recrées les partitions corrompues à travers linéage
  rdd2.persist(StorageLevel.MEMORY_AND_DISK_2)
  rdd2.persist(StorageLevel.DISK_ONLY_2)
  rdd2.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  rdd2.persist(StorageLevel.MEMORY_ONLY_SER_2)
  //il faut toujours préconisé la mémoire que le disque car c'est trés rapide c à d utilisé persist(memory only) ou bien la méthode cache()
  //si la mémoire ne suffit pas il va faloir utilisé mémory only ser et si encore la mémoire ne suffit pas on peut passer au disque c'a d memory and disque ser...
  //aprés avoir une rdd persistée il faut la libérée en utilisant la méthode unpersiste
  rdd2.unpersist(true)


}
