import com.swoop.alchemy.spark.expressions.hll.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
println("load成功")
val sparkConf = new SparkConf()
sparkConf.setMaster("local")   //本地单线程运行
sparkConf.setAppName("testJob")
val sc = new SparkContext(sparkConf)
val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

val textFile = spark.read.textFile("/Users/duanmz/Desktop/x.txt")
textFile.count() // Number of items in this Dataset
import org.apache.spark.sql.functions._
import com.swoop.alchemy.spark.expressions.hll.functions._
println("检查keypoint")
spark.range(100000).select(
  // exact distinct count
  countDistinct("id").as("cntd"),
  // Spark's HLL implementation with default 5% precision
  approx_count_distinct("id").as("anctd_spark_default"),
  // approximate distinct count with default 5% precision
  hll_cardinality(hll_init_agg("id")).as("acntd_default"),
  // approximate distinct counts with custom precision
  map(
    Seq(0.005, 0.02, 0.05, 0.1).flatMap { error =>
      lit(error) :: hll_cardinality(hll_init_agg("id", error)) :: Nil
    }: _*
  ).as("acntd")
).show(false)