
import org.apache.spark.sql.{DataFrame, SparkSession}

object test2 {

  def insertData(spark: SparkSession, tableName: String, tablePath: String):Unit = {
    import spark.implicits._
    //第一步，模拟乘车数据
    import org.apache.hudi.QuickstartUtils._

    val dataGen: DataGenerator = new DataGenerator()
    val inserts = convertToStringList(dataGen.generateInserts(100))

    import scala.collection.JavaConverters._

    val insertDF: DataFrame = spark.read.json(spark.sparkContext.parallelize(inserts.asScala, 2).toDS())

    insertDF.printSchema()
    insertDF.show(10, truncate = false)

    //插入数据到hudi表
  }



  def main(args: Array[String]): Unit = {
    val spark:SparkSession = {
      SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    }
    val tableName:String = "tbl_trip_cow_test"
    val tablePath:String = "/hudi-warehouse/tbl_trip_cow_test"

    //构建数据生成器,模拟产生业务数据
    import org.apache.hudi.QuickstartUtils._

    //任务一:模拟产生业务数据，插入hudi表，采用COW模式
    insertData(spark,tableName,tablePath)

    spark.stop()
  }
}
