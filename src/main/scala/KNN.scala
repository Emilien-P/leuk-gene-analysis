import org.apache.spark.rdd._

class KNN {
  var neighbours : RDD[Gse] = _
  var features : List[Int] = _
  var k = 0
  var initialized = false

  def init(array : RDD[Gse], selected_genes : List[Int], k :Int) : Unit = {
    this.k = k
    neighbours = array
    features = selected_genes
    initialized = true
  }
  type SampleType = String

  def classify(t : Gse)(diff : (Float, Float) => Float) : SampleType = {
    assert(initialized)
    val accumulator : List[(Float, String)] = List.fill(k)((Float.MaxValue, "Init"))

    def seqOp(acc : List[(Float, String)], gse : Gse) : List[(Float, String)] = {
      val dis : Float = LGA.diffOfArrays(gse.geneData, t.geneData)(diff)
      val maxElem = acc.maxBy(_._1)
      if (dis < maxElem._1) {
        acc.updated(acc.indexOf(maxElem), (dis, gse.sampleType))
      }else acc
    }

    neighbours.aggregate(accumulator)()
  }
}
