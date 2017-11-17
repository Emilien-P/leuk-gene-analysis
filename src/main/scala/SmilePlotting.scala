import java.awt.Color
import org.apache.spark.rdd.RDD
import smile.plot._


object SmilePlotting {
  def scatterPlot(features : (Int, Int), data : RDD[(String, Gse)], classes : Array[String]) : Window = {
    val formatted : Array[(Array[Array[Double]], Array[Int])] = for{
      cl <- classes
      x = data.filter{case(str, _) => str.equals(cl)}.values
        .map(gse => Array(gse.geneData(features._1).toDouble, gse.geneData(features._2).toDouble))
        .collect
    } yield (x, Array.fill[Int](x.length)(classes.indexOf(cl)))

    val genesCoor : Array[Array[Double]] = formatted.map(_._1).reduce(_ ++ _)
    val classe : Array[Int] = formatted.map(_._2).reduce(_ ++ _)

    plot(genesCoor, classe, Array('+', 'o'), Array(Color.red, Color.blue))
  }

  def scatterPlot(features : (Int, Int, Int), data : RDD[(String, Gse)], classes : Array[String]) : Window = {
    val formatted : Array[(Array[Array[Double]], Array[Int])] = for{
      cl <- classes
      x = data.filter{case(str, _) => str.equals(cl)}.values
        .map(gse => Array(gse.geneData(features._1).toDouble, gse.geneData(features._2).toDouble, gse.geneData(features._3).toDouble))
        .collect
    } yield (x, Array.fill[Int](x.length)(classes.indexOf(cl)))

    val genesCoor : Array[Array[Double]] = formatted.map(_._1).reduce(_ ++ _)
    val classe : Array[Int] = formatted.map(_._2).reduce(_ ++ _)

    plot(genesCoor, classe, Array('+', 'o'), Array(Color.red, Color.blue))
  }
}