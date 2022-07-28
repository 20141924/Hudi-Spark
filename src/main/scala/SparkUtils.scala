import org.apache.spark.sql.SparkSession

/**
 * SparkSQL 操作数据（加载读取和保存写入）时工具类，比如获取SparkSession实例对象等等
 */
object SparkUtils {

  /**
   * 构建SparkSession实例对象时，默认情况下本地模式运行
   */
  def createSpakSession(clazz: Class[_], master: String = "local[4]", partitions: Int = 4): SparkSession = {
    SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .master("local[4]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", partitions)
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val spark = createSpakSession(this.getClass)
    println(spark)

    Thread.sleep(1000)

    spark.stop()
  }

}

