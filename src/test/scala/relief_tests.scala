import LGA._
import org.scalatest.FunSuite
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

class relief_tests extends FunSuite{
  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("LGA_testing")
  val sc = new SparkContext(conf)

  val test_LGA = new LGA

  test("relief simple test") {
    val test_data = sc.parallelize(
      Seq(("class1", Gse("a", "a", "class1", Array(1, 1, 0, 0))),
        ("class1", Gse("b", "b", "class1", Array(1, 1, 0, 0))),
        ("class2", Gse("c", "c", "class2", Array(0, 0, 0, 0))),
        ("class2", Gse("d", "d", "class2", Array(0, 1, 0, 0))))
    )
    println(LGA.relief(test_data, ("class1", "class2"), 4, 4)(LGA.manhattan).toList)

  }
}
