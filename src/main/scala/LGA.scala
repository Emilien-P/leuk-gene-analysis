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
    val raw_genes = sc.textFile("resources/mile_transposed_preprocessed.csv")
    val raw_genes_splitted : RDD[(String, Array[Float])] = raw_genes
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } //drop header
      .map(_.split(','))
      .map(a => (a(0).filter(_ != '\"'), a.slice(1, a.length).map(_.toFloat)))

    val gse_empty : RDD[(String, Gse)] = gse_info.map(_.split('\t'))
      .map(a => (a(0), Gse(a(0), a(1), a(2), null)))

    val joined : RDD[(String, (Gse, Array[Float]))] = gse_empty.join(raw_genes_splitted)

    val gse_populated : RDD[(String, Gse)]= joined.mapValues{
      case (Gse(id, stitle, stype, _), array) => Gse(id, stitle, stype, array)
    }.persist

    //Group by subtype
    val gse_type : RDD[(String, Gse)] = gse_populated.map{
      case (_, gse @ Gse(_, _, stype, _)) => (stype, gse)
    }

    val gse_grouped : RDD[(String, Iterable[Gse])] = gse_type.groupByKey()

    def saveClassGeneData(className : String) : Unit = {
      gse_populated.filter{
        case (_, Gse(_, _, stype, _)) => stype.equals(className)
      }.map(gse => gse._2.geneData)
        .saveAsTextFile(className + "RawGeneData.csv")
    }
    //KNN training and classifying test with relief genes selection
    def knn_training_test(nb_features : Int = 3, nb_nn : Int = 7, nb_sample : Int = 50, nb_sample_relief : Int = 200) : Unit = {
      val timeStamp : String = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
      val outputWriter = new PrintWriter(new File("results/knn_relief" + timeStamp + ".txt"))
      val class1 : String = "CLL"
      val class2 : String = "CML"
      outputWriter.write("Testing knn with relief with : \n")
      outputWriter.write("\t number of neighbours = " + nb_nn)
      outputWriter.write("\n\t number of features = " + nb_features)
      outputWriter.write("\n\t number of samples tested = " + nb_sample + "\n") //ROC curve ?

      val weights = relief(gse_populated.map(p => (p._2.sampleType, p._2)), (class1, class2), nb_sample_relief, genes_number)(manhattan).toList
      val selected_genes = weights.zipWithIndex.sortBy(-_._1).take(nb_features)
      outputWriter.write("Computed relief weights := " + weights.zipWithIndex.sortBy(-_._1) + "\n") //ROC curve ?

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
        outputWriter.write("indexes of gene of interest: " + selected_genes)
      }

      outputWriter.write("Statistics for selected genes with relief\n")
      testWithGenes(selected_genes, manhattan)
      outputWriter.write("Statistics for random selected genes\n")
      val random_sample_genes =  Random.shuffle(weights.zipWithIndex).take(nb_features)
      outputWriter.write("Statistics for random picked genes\n")
      testWithGenes(random_sample_genes, manhattan)
      outputWriter.close()
    }

    //Loop over parameters to do extensive testing
    def knn_exhaustive(max_nb_features : Int = 3, max_nb_nn : Int = 7, max_nb_sample : Int = 50, step : Int = 25, max_nb_sample_relief : Int = 200) : Unit = {
      val timeStamp: String = new SimpleDateFormat("yyyyMMdd" /*HHmm"*/).format(new Date())
      val outputWriter = new PrintWriter(new File("knn_relief_cor.txt"))
      val class1: String = "CLL"
      val class2: String = "CML"
      outputWriter.write("genes_selection_method,distance_method,nb_features,nb_nn," +
        "class1,class1_nb_tested,class1_true_positive_rate,class2,class2_nb_tested,class2_true_positive_rate," +
        "overall,nb_tested,true_positive_rate\n")

      val dataOfInterest: RDD[Gse] = gse_populated.filter { case (_, Gse(_, _, stype, _)) => stype.equals(class1) || stype.equals(class2) }.values§
      val trainingData: RDD[Gse] = dataOfInterest.zipWithIndex.filter { case (_, idx) => idx % 2 == 0 }.keys.persist()
      val testingData: RDD[Gse] = dataOfInterest.zipWithIndex.filter { case (_, idx) => idx % 2 != 0 }.keys.persist()

      for (
        nb_features <- 1 to max_nb_features;
        nb_nn <- 1 to max_nb_nn by 2;
        nb_sample <- step to max_nb_sample by step;
        nb_sample_relief <- max_nb_sample_relief to max_nb_sample_relief by step
      )
      {
        val weights = relief(dataOfInterest.map(p => (p.sampleType, p)), (class1, class2), nb_sample_relief, genes_number)(manhattan).toList
        val selected_genes = weights.zipWithIndex.sortBy(-_._1).take(nb_features)

        val someSample = testingData.takeSample(withReplacement = false, nb_sample)

        def testWithGenes(genesOfInterest: List[(Float, Int)], diff: (Float, Float) => Float, flags : List[String]): Unit = {
          val knn = new KNN
          //Training the knn-classifier
          knn.init(trainingData, genesOfInterest.unzip._2, nb_nn)
          var nb_success = 0f
          var class1_success = 0f
          var class2_success = 0f
          var nb_class1 = 0
          var nb_class2 = 0

          for (s <- someSample) {
            if (knn.classify(s)(diff).equals(s.sampleType)) {
              nb_success += 1
              if (s.sampleType.equals(class1))
                class1_success += 1
              else
                class2_success += 1
            }
            if (s.sampleType.equals(class1))
              nb_class1 += 1
            else
              nb_class2 += 1
          }
          val method = flags(0)
          val distance = flags(1)
          val class1_tp = class1_success / nb_class1
          val class2_tp = class2_success / nb_class2
          val overall_tp = nb_success / nb_sample
          outputWriter.write(s"$method,$distance,$nb_features,$nb_nn," +
            s"$class1,$nb_class1,$class1_tp," +
            s"$class2,$nb_class2,$class2_tp," +
            s"overall,$nb_sample,$overall_tp\n")
          print("SHOULD PRINT")
        }

        testWithGenes(selected_genes, manhattan, List("relief", "manhattan"))
        val random_sample_genes = Random.shuffle(weights.zipWithIndex).take(nb_features)
        testWithGenes(random_sample_genes, manhattan,  List("random", "manhattan"))
      }
      outputWriter.close()
    }

    //knn_training_test(nb_features=1, nb_sample_relief = 200)
    knn_exhaustive(max_nb_features = 5)

    //SmilePlotting.scatterPlot((6709, 3333, 5492), gse_type, Array("CML", "CLL"))
    //SmilePlotting.scatterPlot((70, 12346), gse_type, Array("CML", "CLL"))
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
  def diffOfElements(a1 : Array[Float], a2 : Array[Float])(fct : (Float, Float) => Float) : Array[Float] = {
    assert(a1.length == a2.length)
    a1.zip(a2).map{s => fct(s._1, s._2)}
  }
  //Relief Algorithm Implementation
  def relief(gse_data : RDD[(String, Gse)], classes : (String, String), n : Int, n_genes : Int)(diff: (Float, Float) => Float) : Array[Float] = {
    val w = new Array[Float](n_genes)
    val gse_together : RDD[Gse] = gse_data.filter{case (s, _) => s.equals(classes._1) || s.equals(classes._2)}.values
    val gse_c1 = gse_data.filter{case (s, _) => s.equals(classes._1)}.persist()
    val gse_c2 = gse_data.filter{case (s, _) => s.equals(classes._2)}.persist()

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

  def kNearest(g : Gse, k : Int, gse_data : Iterable[Gse])(diff: (Float, Float) => Float): List[Gse] ={
    gse_data.toList.sortBy(gse => diffOfArrays(gse.geneData, g.geneData)(diff)).take(k)
  }
  //TODO: TEST RELIEFF EXTENSIVELY
  //ReliefF algorithm implementation allowing multi-class feature selection
  def reliefF(gse_data : RDD[(String, Gse)], n : Int, k: Int, n_genes : Int, n_classes : Int, classesProbabilities : Map[String, Float])(diff: (Float, Float) => Float) : Array[Float] = {
    val w = new Array[Float](n_genes)
    //Group by subtype, costy operation
    val gse_grouped : RDD[(String, Iterable[Gse])] = gse_data.map{
      case (_, gse @ Gse(_, _, stype, _)) => (stype, gse)
    }.groupByKey().persist()

    for(r <- gse_data.takeSample(withReplacement = false, n)){
      //Find the k nearest hits
      val khits : List[Gse] = kNearest(r._2, k, gse_grouped.filter{case (cl, _) => cl == r._1}.values.collect()(0)
        .filter(_.id != r._2.id))(diff)
      //Find the k nearest misses per class
      val kmisses : Map[String, List[Gse]] = gse_grouped.filter{_._1 != r._1}.mapValues(v => kNearest(r._2, k, v)(diff))
        .collect().toMap

      //Compute the weights to be added/subtracted to our weight vector
      val hitsWeight : Array[Float] = khits.map(gse => diffOfElements(r._2.geneData, gse.geneData)(diff))
          .reduce(diffOfElements(_, _)(_ + _)).map(_ / (n * k))

      val pNotR = 1 - classesProbabilities(r._1)

      val missesWeightList : List[Array[Float]] = kmisses.map{
        case(cl, l) =>
          val probFactor = classesProbabilities(cl) / pNotR
          l.map(gse => diffOfElements(r._2.geneData, gse.geneData)(diff))
            .reduce(diffOfElements(_, _)(_ + _)).map(_ * probFactor / (n * k))
      }.toList

      val missesWeight = missesWeightList.reduce(diffOfElements(_, _)(_ + _))

      //update the weight vector
      for(idx <- (0 until n_genes).par){
        w(idx) -= hitsWeight(idx)
        w(idx) += missesWeight(idx)
      }
    }
    w
  }

  def manhattan(f1 : Float, f2 : Float) : Float = Math.abs(f1 - f2)

}