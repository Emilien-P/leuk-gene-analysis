import LGA._
import org.scalatest.FunSuite
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random._

class SyntheticDataTests extends FunSuite{
  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("LGA_testing")
  val sc = new SparkContext(conf)

  val test_LGA = new LGA

  def generateSynthetic(nb_classes : Int) : Array[Array[Float]] = {
    val nb_gene = 20
    val data = Array.ofDim[Float](nb_gene, nb_gene * nb_classes)
    data
  }

}
