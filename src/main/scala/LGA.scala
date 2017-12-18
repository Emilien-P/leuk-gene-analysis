import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}
import Math._
import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import breeze.stats._

import scala.util.Random
import smile.regression._
import smile.validation._

import scala.annotation.tailrec

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

    val geneIdToIndex  = raw_genes.take(1)(0).split(',').drop(1).zipWithIndex.toMap
    //Loading a transposed version of the data, not optimal but easier for now
    //Only used for correlation measurement
    val raw_genes_array : RDD[String] = sc.textFile("resources/mile_cleaned.csv")

    val raw_genes_array_splited : RDD[Array[Float]] = raw_genes_array
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } //drop header
      .map(_.split(','))
      .map(a => a.slice(1, a.length).map(_.toFloat)).persist()


    val gse_empty : RDD[(String, Gse)] = gse_info.map(_.split('\t'))
      .map(a => (a(0), Gse(a(0), a(1), a(2), null)))

    val joined : RDD[(String, (Gse, Array[Float]))] = gse_empty.join(raw_genes_splitted)

    val gse_populated : RDD[(String, Gse)]= joined.mapValues{
      case (Gse(id, stitle, stype, _), array) => Gse(id, stitle, stype, array)
    }//.persist

    //Group by subtype
    val gse_type : RDD[(String, Gse)] = gse_populated.map{
      case (_, gse @ Gse(_, _, stype, _)) => (stype, gse)
    }.persist()

    val gse_grouped : RDD[(String, Iterable[Gse])] = gse_type.groupByKey()

    def saveClassGeneData(className : String) : Unit = {
      gse_populated.filter{
        case (_, Gse(_, _, stype, _)) => stype.equals(className)
      }.map(gse => gse._2.geneData)
        .saveAsTextFile(className + "RawGeneData.csv")
    }

    def lassoRegression(lambda : Int = 100) : Unit= {
      val mapClassInt : Map[String, Int] = gse_grouped.keys.collect.zipWithIndex.toMap
      val pair : Array[(Array[Double], Int)] = gse_populated.mapValues(gse => (gse.geneData.map(_.toDouble), mapClassInt(gse.sampleType))).values.collect()
      val classesLabels : Array[Int] = pair.unzip._2
      val geneDataArray : Array[Array[Double]] = pair.unzip._1

      val lassoModel = lasso(geneDataArray, classesLabels.map(_.toDouble), lambda)

      val timeStamp : String = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
      val outputWriter = new PrintWriter(new File("results/lasso_output" + timeStamp + ".txt"))
      outputWriter.write(lassoModel.toString)
      outputWriter.close()
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

      val dataOfInterest: RDD[Gse] = gse_populated.filter { case (_, Gse(_, _, stype, _)) => stype.equals(class1) || stype.equals(class2) }.values
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
        }

        testWithGenes(selected_genes, manhattan, List("relief", "manhattan"))
        val random_sample_genes = Random.shuffle(weights.zipWithIndex).take(nb_features)
        testWithGenes(random_sample_genes, manhattan,  List("random", "manhattan"))
      }
      outputWriter.close()
    }

    def reliefF_output(n : Int = 25) : Unit = {
      val timeStamp : String = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
      val outputWriter = new PrintWriter(new File("results/reliefF_output" + timeStamp + ".txt"))
      val weights : Array[Float] = reliefF_print(gse_type, n, n_genes = genes_number, classesP = Option.empty)(manhattan)
      val selected_genes = weights.zipWithIndex.sortBy(-_._1)
      outputWriter.write("Computed relief weights := " + selected_genes.toList.toString() + "\n")
      outputWriter.close()
    }

    def iterativeVersusNormalTest(nb_features : Int = 3, nb_nn : Int = 17) : Unit= {
      val timeStamp : String = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
      val outputWriter = new PrintWriter(new File("results/ext_" + nb_features + "_" + nb_nn + ".txt"))

      val setClasses : Set[String] = Set("MDS", "CLL")
      val listClasses = setClasses.toList
      outputWriter.write("Training and testing with : " + setClasses + "\n")
      val gse_filtered = gse_type.filter{case (typ, _) => setClasses.contains(typ)}
      val Array(testing, training) = gse_filtered.randomSplit(Array(.2, .8))

      val testing_grouped = testing.groupByKey().persist()

      val minTest = testing_grouped.mapValues(_.size).collect().unzip._2.min
      val testing_equil = testing_grouped.mapValues(_.take(minTest))

      for (c <- setClasses){
        outputWriter.write("Number of sample for " + c +" :" + testing_equil.filter{case(s, _) => s.equals(c)}.values.count + "\n")
      }
      val iterativeList : List[Int]= iterativeReliefF(raw_genes_array_splited, geneIdToIndex, nb_features, training, 150 ,n_genes = genes_number,
        thresh = 0.75f, classesP = Option.empty)(manhattan).unzip._2.toList

      val weightList = reliefF(training, 200, genes_number, Option.empty)(manhattan)
      val normalList : List[Int] = weightList.zipWithIndex.sortBy(-_._1).take(nb_features).unzip._2.toList

      def testWithGenesHomemade(genesOfInterest: List[Int], classes : Set[String], diff: (Float, Float) => Float, flags : List[String]): Unit = {
        val knn = new KNN
        //Training the knn-classifier
        knn.init(training.values, genesOfInterest, nb_nn)
        val testingData : Array[Gse] = testing_equil.values.collect().flatten
        val pred : Array[Int] = testingData.map(knn.classify(_)(diff)).map(listClasses.indexOf(_))
        val truth : Array[Int] = testingData.map{case Gse(_, _, typ, _) => listClasses.indexOf(typ)}
        outputWriter.write(pred.zip(truth).toList + "\n")
        val accuracy = smile.validation.accuracy(truth , pred)
        //val precision = smile.validation.precision(truth, pred)
        //val falseDisc = smile.validation.fallout(truth, pred)
        val method = flags(0)
        outputWriter.write(s"Accuracy for $method : " + accuracy + "\n")
        //outputWriter.write(s"Precision for $method : " + precision + "\n")
        //outputWriter.write(s"False discovery rate for $method : " + falseDisc + "\n")
      }

      def testWithGenes(genesOfInterest: List[Int], classes : Set[String], diff: (Float, Float) => Float, flags : List[String]): Unit = {
        val filtered : Array[(String, Array[Double])] = training.mapValues{case Gse(_, _, _, data) =>
          data.zipWithIndex.filter{case (_, idx) => genesOfInterest.toSet.contains(idx)}
              .unzip._1.map(_.toDouble)
        }.collect()
        val x = filtered.unzip._2
        val y = filtered.unzip._1.map(listClasses.indexOf(_))
        print(y.toList)
        val knn = smile.classification.knn(x, y, nb_nn)
        //Training the knn-classifier
        val testingData : Array[Gse] = testing_equil.values.collect().flatten
        val pred : Array[Int] = testingData.map(onlySelectedGenes(_, genesOfInterest.toSet))
          .map(x => knn.predict(x))
        val truth : Array[Int] = testingData.map{case Gse(_, _, typ, _) => listClasses.indexOf(typ)}
        outputWriter.write(pred.zip(truth).toList + "\n")

        if(nb_features == 2)
          smile.plot.plot(x, y, smile.classification.knn(x, y, nb_nn))

        val accuracy = smile.validation.accuracy(truth , pred)
        //val precision = smile.validation.precision(truth, pred)
        //val falseDisc = smile.validation.fallout(truth, pred)
        val method = flags(0)
        outputWriter.write(s"Accuracy for $method : " + accuracy + "\n")
        //outputWriter.write(s"Precision for $method : " + precision + "\n")
        //outputWriter.write(s"False discovery rate for $method : " + falseDisc + "\n")
      }

      testWithGenes(normalList, setClasses, manhattan, List("Normal"))
      testWithGenes(iterativeList, setClasses, manhattan, List("Iterative"))
      outputWriter.close()
    }

    def corrMatOf(selected : List[Int], label  : String = "Unknown") : Unit= {
      val timeStamp : String = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
      val outputWriter = new PrintWriter(new File(s"results/CovMat" + timeStamp + ".txt"))
      outputWriter.write("Cov Matrix of those genes : " + selected + "\n")
      outputWriter.write(s"Method : $label\n")
      outputWriter.write(LGA.corrMatrix(raw_genes_array_splited, selected).toString())
      outputWriter.close()
    }

    def exhaustiveNormalVsIterative(nb_features : (Int, Int), nb_neighbors : (Int, Int)) : Unit = {
      val timeStamp : String = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
      val outputWriter = new PrintWriter(new File("results/iterative_normal_ex" + timeStamp + ".txt"))

      val setClasses : Set[String] = Set("MDS", "CLL", "CML", "T-ALL")
      val listClasses = setClasses.toList
      outputWriter.write("Training and testing with : " + setClasses + "\n")
      val gse_filtered = gse_type.filter{case (typ, _) => setClasses.contains(typ)}
      val Array(testing, training) = gse_filtered.randomSplit(Array(.2, .8))

      val testing_grouped = testing.groupByKey().persist()

      val minTest = testing_grouped.mapValues(_.size).collect().unzip._2.min
      val testing_equil = testing_grouped.mapValues(_.take(minTest)).persist()

      def testWithGenes(genesOfInterest: List[Int], classes : Set[String], diff: (Float, Float) => Float,
                        nb_nn :Int, flags : List[String]): Double = {
        val filtered : Array[(String, Array[Double])] = training.mapValues{case Gse(_, _, _, data) =>
          data.zipWithIndex.filter{case (_, idx) => genesOfInterest.toSet.contains(idx)}
            .unzip._1.map(_.toDouble)
        }.collect()
        val x : Array[Array[Double]]= filtered.unzip._2
        val y : Array[Int] = filtered.unzip._1.map(listClasses.indexOf(_))
        val knn = smile.classification.randomForest(x, y)
        //Training the classifier
        val testingData : Array[Gse] = testing_equil.values.collect().flatten
        val pred : Array[Int] = testingData.map(onlySelectedGenes(_, genesOfInterest.toSet))
          .map(x => knn.predict(x))
        val truth : Array[Int] = testingData.map{case Gse(_, _, typ, _) => listClasses.indexOf(typ)}
        val accuracy = smile.validation.accuracy(truth , pred)
        accuracy
      }

      val iterativeList1 : List[Int]= iterativeReliefF(raw_genes_array_splited, geneIdToIndex, nb_features._2, training, 300 ,n_genes = genes_number,
        thresh = 0.75f, classesP = Option.empty)(manhattan).unzip._2.toList


      for(nb_f <- nb_features._1 to nb_features._2){
        for(nb_nn <- nb_neighbors._1 until nb_neighbors._2 by 2){
          val accuracy : Double = testWithGenes(iterativeList1.take(nb_f), setClasses, manhattan, nb_nn, List("Iterative"))

          outputWriter.write(accuracy + ",")
        }
        outputWriter.write("\n")
      }
      outputWriter.close()
    }

    def chiSquareRatio(nb_features : (Int, Int), nb_neighbors : (Int, Int)) : Unit = {
      val timeStamp : String = new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
      val outputWriter = new PrintWriter(new File("results/chi_square_ex" + timeStamp + ".txt"))

      val setClasses : Set[String] = Set("MDS", "CLL", "CML", "T-ALL")
      val listClasses = setClasses.toList
      outputWriter.write("Training and testing with : " + setClasses + "\n")
      val gse_filtered = gse_type.filter{case (typ, _) => setClasses.contains(typ)}
      val Array(testing, training) = gse_filtered.randomSplit(Array(.2, .8))

      val testing_grouped = testing.groupByKey().persist()

      val minTest = testing_grouped.mapValues(_.size).collect().unzip._2.min
      val testing_equil = testing_grouped.mapValues(_.take(minTest)).persist()

      val train_array : Array[(String, Array[Double])] = training.mapValues{case Gse(_, _, _, data) =>
        data.zipWithIndex.unzip._1.map(_.toDouble)
      }.collect()

      def testWithGenes(genesOfInterest: List[Int], classes : Set[String], diff: (Float, Float) => Float,
                        nb_nn :Int, flags : List[String]): Double = {
        val filtered : Array[(String, Array[Double])] = train_array
          .map{case (str, data) => (str, data.zipWithIndex
            .filter{case (_, idx) => genesOfInterest.toSet.contains(idx)}.unzip._1)}

        val x : Array[Array[Double]]= filtered.unzip._2
        val y : Array[Int] = filtered.unzip._1.map(listClasses.indexOf(_))
        val knn = smile.classification.randomForest(x, y)
        //Training the classifier
        val testingData : Array[Gse] = testing_equil.values.collect().flatten
        val pred : Array[Int] = testingData.map(onlySelectedGenes(_, genesOfInterest.toSet))
          .map(x => knn.predict(x))
        val truth : Array[Int] = testingData.map{case Gse(_, _, typ, _) => listClasses.indexOf(typ)}
        val accuracy = smile.validation.accuracy(truth , pred)
        accuracy
      }

      val x = train_array.unzip._2

      val y = train_array.unzip._1.map(listClasses.indexOf(_))

      val scores : Array[Double] = smile.feature.sumSquaresRatio(x, y)

      val features : List[Int] = scores.zipWithIndex.sortBy(-_._1).unzip._2.toList

      for(nb_f <- nb_features._1 to nb_features._2){
          val accuracy : Double = testWithGenes(features.take(nb_f),setClasses, manhattan, 0, List("ChiSquared"))
          outputWriter.write(accuracy + ",")
        outputWriter.write("\n")
      }
      outputWriter.close()
    }


    //knn_training_test(nb_features=1, nb_sample_relief = 200)
    //knn_exhaustive(max_nb_features = 5)

    //reliefF_output(n = 250)
    /**val selectedTypes = Set("MDS", "CML", "CLL")
    val weights : Array[Float] = reliefF(gse_type.filter{case(s, _) => selectedTypes.contains(s)},
      1000, n_genes = genes_number, classesP = Option.empty)(manhattan)
    val selected_genes = weights.zipWithIndex.sortBy(-_._1).take(5).unzip._2.toList
    corrMatOf(selected_genes, "ReliefF with n= 100")**/

    //lassoRegression()

    //SmilePlotting.featurePlot(List(6709, 3333), gse_type, Array("CML", "CLL"))
    //SmilePlotting.featurePlot(List(70, 12346), gse_type, Array("CML", "CLL"))

    exhaustiveNormalVsIterative((1, 30), (1, 2))

    //chiSquareRatio((1, 20), (1, 21))

    //corrMatOf(List(17503, 7171, 8539, 17037, 16362))
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

  def onlySelectedGenes(gse : Gse, selectedGenes : Set[Int]) : Array[Double] = gse match {
    case Gse(id, title, stype, data) =>
      data.zipWithIndex.filter{case (_, idx) => selectedGenes.contains(idx)}.unzip._1.map(_.toDouble)
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

  //Implement Heap/Set for kNearest, this is really expensive but works just as testing purpose
  def kNearest(g : Gse, k : Int, gse_data : Iterable[Gse])(diff: (Float, Float) => Float): List[Gse] = {
    gse_data.toList.sortBy(gse => diffOfArrays(gse.geneData, g.geneData)(diff)).take(k)
  }

  //ReliefF algorithm implementation allowing multi-class feature selection
  //k = 10 is an empirical good choice
  def reliefF(gse_data : RDD[(String, Gse)], n : Int, n_genes : Int, classesP : Option[Map[String, Float]], k: Int = 10)(diff: (Float, Float) => Float) : Array[Float] = {
    val w = new Array[Float](n_genes)
    //Group by subtype, costly operation
    val gse_grouped : RDD[(String, Iterable[Gse])] = gse_data.map{
      case (_, gse @ Gse(_, _, stype, _)) => (stype, gse)
    }.groupByKey().persist()

    //If the prior class probabilities are not provided, compute them from the data set
    val classesProbabilities : Map[String, Float] = if(classesP.isEmpty){
      val mapped = gse_grouped.mapValues(_.toList.size)
      val number_sample = mapped.values.reduce(_ + _).toFloat
      mapped.mapValues(_ / number_sample).collect.toMap
    }else classesP.get

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

  def reliefF_print(gse_data : RDD[(String, Gse)], n : Int, k: Int = 10, n_genes : Int, classesP : Option[Map[String, Float]])(diff: (Float, Float) => Float) : Array[Float] = {
    val writer = new PrintWriter(new File("results/reliefF_stability_" + n + ".txt"))
    var cnt = 0
    val w = new Array[Float](n_genes)
    //Group by subtype, costly operation
    val gse_grouped : RDD[(String, Iterable[Gse])] = gse_data.map{
      case (_, gse @ Gse(_, _, stype, _)) => (stype, gse)
    }.groupByKey().persist()

    //If the prior class probabilities are not provided, compute them from the data set
    val classesProbabilities : Map[String, Float] = if(classesP.isEmpty){
      val mapped = gse_grouped.mapValues(_.toList.size)
      val number_sample = mapped.values.reduce(_ + _).toFloat
      mapped.mapValues(_ / number_sample).collect.toMap
    }else classesP.get

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
      writer.write(cnt + ",")
      for (idx <- 0 until n_genes) {
        writer.write(w(idx) + ",")
      }
      writer.write("\n")
      cnt += 1
    }
    writer.close()
    w
  }

  def iterativeReliefF(raw_gene_data : RDD[Array[Float]], geneIdToIdx : Map[String, Int], m : Int, gse_data : RDD[(String, Gse)], n : Int, k: Int = 10,
                       thresh : Float = 0.8f, n_genes : Int, classesP : Option[Map[String, Float]])
                      (diff: (Float, Float) => Float) : Set[(Float, Int)] = {
    var w = Array.emptyFloatArray
    val toKeep = (0 until n_genes).toSet
    val raw_data = raw_gene_data.collect()

    @tailrec
    def recursiveHelper(m : Int, selected : Set[(Float, Int)], toKeep : Set[Int], n_genes : Int) : Set[(Float, Int)] = {
      if (m == 0) {
        selected
      }else{
        val w = reliefF(gse_data.mapValues{
          case Gse(id, title, stype, data) => Gse(id, title, stype,
            data.zipWithIndex.filter{case(_, idx) => toKeep.contains(idx)}.unzip._1)
        },
          n, n_genes, classesP, k)(diff)
        val sorted : List[(Float, Int)] = w.zipWithIndex.sortBy(-_._1).take(10).toList
        val topGene : (Float, Int) = sorted.filter{case(_, idx) => !selected.unzip._2.contains(idx)}.head
        val topIdx = topGene._2
        val topStd = stddev(raw_data(topIdx))
        var toRemove : Set[Int] = Set.empty
        for(j <- toKeep){
          val elemStd = stddev(raw_data(j))
          val corr : Double = DescriptiveStats.cov[Float](raw_data(topIdx), raw_data(j)) match {
            case f : Float => f / (topStd * elemStd)
          }
          if (corr >= thresh && j != topIdx) {
            toRemove = toRemove + j
          }
        }
        recursiveHelper(m - 1, selected + topGene, toKeep -- toRemove, n_genes - toRemove.size)
      }
    }
    recursiveHelper(m, Set.empty, toKeep, n_genes)
  }

  def corrMatrix(raw_gene_data : RDD[Array[Float]], selectedGenes : List[Int]) : List[List[Double]] = {
    val raw_data: Array[Array[Float]] = raw_gene_data.collect()
    (for {
      idx1: Int <- selectedGenes
      std1 = stddev(raw_data(idx1))
    } yield (for {
      idx2: Int <- selectedGenes
      std2 = stddev(raw_data(idx2))
    } yield (DescriptiveStats.cov[Float](raw_data(idx1), raw_data(idx2)), std1 * std2)).toList
        .map{case (x : Float, y : Double) => x / y}).toList
  }
  def manhattan(f1 : Float, f2 : Float) : Float = Math.abs(f1 - f2)

}