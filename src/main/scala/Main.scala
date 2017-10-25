import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("LGA")
    val sc = new SparkContext(conf)

    //Load the data into RDDs
    val gse_info : RDD[String] = sc.textFile("resources/GSE13159.info.txt")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } //drop header
    val raw_genes = sc.textFile("resources/mile_transposed.csv")
    val raw_genes_splitted : RDD[(String, Array[Float])] = raw_genes
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } //drop header
      .map(_.split(','))
      .map(a => (a(0).filter(_ != '\"'), a.slice(1, a.length).map(_.toFloat)))

    //Group the gse_info by subtype
    case class Gse (id : String, sampleTitle : String, sampleType : String, geneData : Array[Float])

    val gse_empty : RDD[(String, Gse)] = gse_info.map(_.split('\t'))
      .map(a => (a(0), Gse(a(0), a(1), a(2), null)))

    val joined : RDD[(String, (Gse, Array[Float]))] = gse_empty.join(raw_genes_splitted)

    val gse_populated : RDD[(String, Gse)]= joined.mapValues{
      case (Gse(id, stitle, stype, _), array) => Gse(id, stitle, stype, array)
    }

  }

}