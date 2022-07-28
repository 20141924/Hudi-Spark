import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
/*
可运行
PRECOMBINE_FIELD_OPT_KEY（必填）：当同一批次中的两条记录具有相同的键值时，将选择指定字段中值最大的记录。
ts一定要有
 */
object test5 {
  def insertData(spark: SparkSession, table: String, path: String):Unit = {
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
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._

    insertDF.write
      .mode(SaveMode.Append)
      .format("hudi")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option(PRECOMBINE_FIELD.key(),"ts")
      .option(RECORDKEY_FIELD.key(),"uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TBL_NAME.key(), table)
      .save(path)
  }



  def main(args: Array[String]): Unit = {
    val spark:SparkSession = {
      SparkSession.builder()
        .appName(this.getClass.getSimpleName.stripSuffix("$"))
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    }
    val tableName:String = "tbl_trip_cow_test3"
    val tablePath:String = "/mzx/tbl_trip_cow_test3"

    //构建数据生成器,模拟产生业务数据
    import org.apache.hudi.QuickstartUtils._

    //任务一:模拟产生业务数据，插入hudi表，采用COW模式
    insertData(spark,tableName,tablePath)

    spark.stop()
  }

}
