import java.awt.Color
import org.apache.spark.rdd.RDD
import smile.plot._


object SmilePlotting {
  def featurePlot(features : List[Int] , data : RDD[(String, Gse)], classes : Array[String]) : Window = {
    val formatted : Array[(Array[Array[Double]], Array[Int])] = for{
      cl <- classes
      x = data.filter{case(str, _) => str.equals(cl)}.values
        .map(gse => gse.geneData.zipWithIndex.filter{case (_, idx) => features.contains(idx)}.unzip._1.map(_.toDouble))
        .collect
    } yield (x, Array.fill[Int](x.length)(classes.indexOf(cl)))

    val genesCoor : Array[Array[Double]] = formatted.map(_._1).reduce(_ ++ _)
    val classe : Array[Int] = formatted.map(_._2).reduce(_ ++ _)

    plot(genesCoor, classe, Array('+', 'o'), Array(Color.red, Color.blue))
  }
}