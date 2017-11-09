import org.apache.spark.rdd._

class KNN extends Serializable{
  var neighbours : RDD[Gse] = _
  var features : Set[Int] = _
  var k = 0
  var initialized = false

  def init(array : RDD[Gse], selected_genes : List[Int], k :Int) : Unit = {
    this.k = k
    neighbours = array
    features = selected_genes.toSet
    initialized = true
  }
  type SampleType = String

  def filterGenes(a : Gse, selected_genes : Set[Int]) : Gse = a match { //Set is important to get O(1) get ops
    case Gse(id, sampleTitle, sampleType, geneData) =>
      Gse(id, sampleTitle, sampleTitle, geneData.zipWithIndex.filter{case(_, idx) => selected_genes.contains(idx)}.unzip._1)
  }

  def classify(t : Gse)(diff : (Float, Float) => Float) : SampleType = {
    assert(initialized)
    val accumulator : List[(Float, String)] = List.fill(k)((Float.MaxValue, "Init"))


    def seqOp(acc : List[(Float, String)], gse : Gse) : List[(Float, String)] = {
      val dis : Float = LGA.diffOfArrays(filterGenes(gse, features).geneData, filterGenes(t, features).geneData)(diff) //Use ONLY selected genes
      val maxElem = acc.maxBy(_._1)
      if (dis < maxElem._1) {
        acc.updated(acc.indexOf(maxElem), (dis, gse.sampleType))
      }else acc
    }

    def combOp(l1 : List[(Float, String)], l2 : List[(Float, String)]) : List[(Float, String)] = {
      (l1 ::: l2).sortBy(_._1).take(l1.length)
    }
    neighbours.aggregate(accumulator)(seqOp, combOp)
        .groupBy(identity).maxBy(_._2.length)._1._2
  }
}
