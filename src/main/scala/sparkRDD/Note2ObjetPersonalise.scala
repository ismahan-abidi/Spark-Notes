package tuto0
object Note2ObjetPersonalise extends App {
  //objet personnalisé càd un objet d'une classe que j'ai créer
  class Personne(var nom : String, var prenom : String, val cin : Int){
    //pour caster un objet en scala en utilise asInstanceOf
    override def equals(obj: Any) : Boolean= { val pers:Personne =obj.asInstanceOf[Personne]
      return this.nom.equals(pers.nom)&&this.cin.equals(pers.cin) // return en scala est facultatif
    }
    override def hashCode(): Int = cin
    override def toString: String = s"(nom =$nom, prenom = $prenom, cin = $cin)"

  }
  val x = new Personne("kouki","monem",123)
  val y = new Personne("kouki","ismahan",123)
  if(x.equals(y)){
  //pour afficher un objet dans un println il faut overriding la méthode toString
  println(s"les deux objets $x et $y sont égaux")
}
else println(s"les deux objets $x et $y  sont différents ")


}
