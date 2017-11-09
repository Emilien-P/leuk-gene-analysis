import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import Math._
import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

case class Gse (id : String, sampleTitle : String, sampleType : String, geneData : Array[Float])

object LGA extends LGA {

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("LGA")
    val sc = new SparkContext(conf)
    val genes_number : Int = 17788

    //Load the data into RDDs
    val gse_info : RDD[String] = sc.textFile("resources/GSE13159.info.txt")
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } //drop header
    val raw_genes = sc.textFile("resources/mile_transposed.csv")
    val raw_genes_splitted : RDD[(String, Array[Float])] = raw_genes
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } //drop header
      .map(_.split(','))
      .map(a => (a(0).filter(_ != '\"'), a.slice(1, a.length).map(_.toFloat)))

    //Group the gse_info by subtype
    //TODO: Check the mean and variance of the gene expression level. Should normalize them

    val gse_empty : RDD[(String, Gse)] = gse_info.map(_.split('\t'))
      .map(a => (a(0), Gse(a(0), a(1), a(2), null)))

    val joined : RDD[(String, (Gse, Array[Float]))] = gse_empty.join(raw_genes_splitted)

    val gse_populated : RDD[(String, Gse)]= joined.mapValues{
      case (Gse(id, stitle, stype, _), array) => Gse(id, stitle, stype, array)
    }.persist

    //Group by subtype
    val gse_grouped : RDD[(String, Iterable[Gse])] = gse_populated.map{
      case (_, gse @ Gse(_, _, stype, _)) => (stype, gse)
    }.groupByKey()


    //KNN training and classifying test with relief genes selection
    def knn_training_test() : Unit = {
      val timeStamp : String = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
      val outputWriter = new PrintWriter(new File("results/knn_relief" + timeStamp + ".txt"))
      val nb_features = 3
      val nb_nn = 20
      val nb_sample = 25
      val class1 : String = "CLL"
      val class2 : String = "CML"
      outputWriter.write("Testing knn with relief with : \n")
      outputWriter.write("\t number of neighbours = " + nb_nn)
      outputWriter.write("\n\t number of features = " + nb_features)
      outputWriter.write("\n\t number of samples tested = " + nb_sample + "\n") //ROC curve ?

      val weights = relief(gse_populated.map(p => (p._2.sampleType, p._2)), (class1, class2), 200, genes_number)(manhattan).toList
      val selected_genes = weights.zipWithIndex.sortBy(_._1).take(nb_features)

      val dataOfInterest : RDD[Gse] = gse_populated.filter{case (_, Gse(_, _, stype, _)) => stype.equals(class1) || stype.equals(class2)}.values
      val trainingData : RDD[Gse] = dataOfInterest.zipWithIndex.filter{case (_, idx) => idx % 2 == 0}.keys.persist()
      val testingData : RDD[Gse] = dataOfInterest.zipWithIndex.filter{case (_, idx) => idx % 2 != 0}.keys.persist()

      val someSample = testingData.takeSample(withReplacement = false, nb_sample)

      def testWithGenes(genesOfInterest : List[(Float, Int)], diff : (Float, Float) => Float) : Unit = {
        val knn = new KNN
        //Training the knn-classifier
        knn.init(trainingData, genesOfInterest.unzip._2, nb_nn)
        var nb_success = 0f
        var cll_success = 0f
        var cml_success = 0f
        var nb_cll = 0
        var nb_cml = 0

        for (s <- someSample) {
          if (knn.classify(s)(diff).equals(s.sampleType)) {
            nb_success += 1
            if (s.sampleType.equals(class1))
              cll_success += 1
            else
              cml_success += 1
          }
          if (s.sampleType.equals(class1))
            nb_cll += 1
          else
            nb_cml += 1
          print(knn.classify(s)(diff) + ": actual = " + s.sampleType)
        }
        outputWriter.write("success percentage : " + nb_success / nb_sample.toFloat + "\n")
        outputWriter.write(class1 + " success percentage : " + cll_success / nb_cll + "\n")
        outputWriter.write(class2 + " success percentage : " + cml_success / nb_cml + "\n")
      }

      outputWriter.write("Statistics for selected genes with relief\n")
      testWithGenes(selected_genes, manhattan)
      outputWriter.write("Statistics for random selected genes\n")
      val random_sample_genes =  Random.shuffle(weights.zipWithIndex).take(nb_features)
      outputWriter.write("Statistics for random picked genes\n")
      testWithGenes(random_sample_genes, manhattan)
      outputWriter.close()
    }

    knn_training_test()
  }

}

class LGA extends Serializable {
  def diffOfArrays(a1 : Array[Float], a2 : Array[Float])(fct : (Float, Float) => Float) : Float = {
    var w = 0f
    for (i <- 0 until min(a1.length, a2.length)) {
      w += fct(a1(i), a2(i))
    }
    w
  }
  //Relief Algorithm Implementation
  def relief(gse_data : RDD[(String, Gse)], classes : (String, String), n : Int, n_genes : Int)(diff: (Float, Float) => Float) : Array[Float] = {
    val w = new Array[Float](n_genes)
    val gse_together : RDD[Gse] = gse_data.filter{case (s, _) => s.equals(classes._1) || s.equals(classes._2)}.values
    val gse_c1 = gse_data.filter{case (s, _) => s.equals(classes._1)}
    val gse_c2 = gse_data.filter{case (s, _) => s.equals(classes._2)}

    def nearestN(neighbours : RDD[(String, Gse)], r : Gse) : Gse = {
      def nearer(g1 : Gse, g2 : Gse) : Gse = {
        if (diffOfArrays(g1.geneData, r.geneData)(diff) - diffOfArrays(g2.geneData, r.geneData)(diff) >= 0)
          g1
        else
          g2
      }
      neighbours.reduceByKey(nearer).take(1)(0)._2
    }

    for (r <- gse_together.takeSample(withReplacement = false, n)){
      val h = nearestN(gse_c1.filter(_._2.sampleTitle != r.sampleTitle), r).geneData
      val m = nearestN(gse_c2.filter(_._2.sampleTitle != r.sampleTitle), r).geneData
      for (idx <- (0 until n_genes).par){
        w(idx) -= diff(h(idx), r.geneData(idx)) / n
        w(idx) += diff(m(idx), r.geneData(idx)) / n
      }
    }
    w
  }
  //RefiefF algorithm implementation allowing multi-class feature selection
  def reliefF(gse_data : RDD[(String, Gse)], classes : (String, String), n : Int, n_genes : Int)(diff: (Float, Float) => Float) : Array[Float] = {
    val w = new Array[Float](n_genes)
    null
  }

  def manhattan(f1 : Float, f2 : Float) : Float = Math.abs(f1 - f2)

}