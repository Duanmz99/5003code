import com.swoop.alchemy.spark.expressions.hll.functions._
import org.apache.spark.sql.functions.{approx_count_distinct, countDistinct, lit, map}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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

val rdd = sc.textFile("/Users/duanmz/untitled1/src/main/scala/novel.txt")
val mapRDD= rdd.map(line=>Row(line.split(" ")(0),line.split(" ")(1)))
val sf1=new StructField("id",StringType,true) //这里是列1   信息是 IP
val sf2=new StructField("user",StringType,true) //这里是列2  信息是user

val table_sch=new StructType(Array(sf1,sf2)) //生成表结构 , 由两列叫IP和user的列组成的表 ,可以为空

val df=spark.createDataFrame(mapRDD,table_sch)  //用mapRDD的分列数据去映射到 结构表里面,生成具有列信息的表
println("output 变量类型")
print(df.getClass.toString())

//df.select(
//  countDistinct("id").as("cntd"),
//  approx_count_distinct("id").as("anctd_spark_default"),
//  hll_cardinality(hll_init_agg("id")).as("acntd_default"),
//).show()
//df.createTempView("cyber")            //创建视图 cyber
//
//spark.sql("select * from cyber").show()    //打印视图(表)
df.select(
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
println("执行完毕")
