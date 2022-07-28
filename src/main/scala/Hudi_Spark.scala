import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import java.util.{Calendar, Date, Properties}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.expressions.UserDefinedFunction
/*
* 滴滴海口出行运营数据分析，使用SparkSQL操作数据，加载Hudi表数据，按照业务需求统计
 */
object Hudi_Spark {
  // Hudi表属性，存储数据HDFS路径，这里暂时用本地路径
  val hudiTablePath = "/mzx/tbl_didi_haikou_test"

  val prop = new Properties()
  prop.put("user", "root")
  prop.put("password", "123456")
  prop.put("driver","com.mysql.jdbc.Driver")

  /**
   * 加载Hudi表数据，封装到DataFrame中
   */
  def readFromHudi(spark: SparkSession,path:String):DataFrame={
    val didiDF = spark.read.format("hudi").load(path)
    // 选择字段
    didiDF.select(
      "product_id","type","traffic_type","pre_total_fee","start_dest_distance","departure_time"
    )
  }

  /**
   * 订单类型统计，字段：product_id
   */
  def reportProduct(dataFrame: DataFrame):Unit={
    val reportDF:DataFrame = dataFrame.groupBy("product_id").count()
//    val reportDF2= dataFrame.filter("pre_total_fee>=0").count()
    reportDF.show()
    val transfer_name = udf(
      (productId: Int) => {
        productId match {
          case 1 => "滴滴专车"
          case 2 => "滴滴企业专车"
          case 3 => "滴滴快车"
          case 4 => "滴滴企业快车"
        }
      }
    )

    val resultDF:DataFrame = reportDF.select(
      transfer_name(col("product_id")).as("order_type"),
      col("count").as("total"))
    resultDF.printSchema()
    reportDF.show(10,truncate = false)
//    print(reportDF2)
  }

  def reportPrice(dataFrame: DataFrame):Unit={
    val resultDF:DataFrame = dataFrame
      .agg(
        //价格0-15
        sum(
          when(col("pre_total_fee").between(0,15),1).otherwise(0)
        ).as("0~15"),
        sum(
          when(col("pre_total_fee").between(16,30),1).otherwise(0)
        ).as("16~30"),
        sum(
          when(col("pre_total_fee").between(31,50),1).otherwise(0)
        ).as("31~50"),
        sum(
          when(col("pre_total_fee").between(51,100),1).otherwise(0)
        ).as("51~100"),
        sum(
          when(col("pre_total_fee").gt(100),1).otherwise(0)
        ).as("100+")
      )
    resultDF.printSchema()
    resultDF.show(10,truncate = false)
    //写入数据库
    resultDF.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/test?serverTimezone=UTC&characterEncoding=utf-8&useSSL=false","test.order"
    ,prop)
  }
  /**
   * 订单星期分组统计，先将日期转换为星期，再对星期分组统计，使用字段：departure_time
   */
  def reportWeek(dataFrame: DataFrame):Unit={
    val to_week:UserDefinedFunction = udf(
      (dateStr:String)=>{
        val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        val calendar: Calendar = Calendar.getInstance();

        val date: Date = format.parse(dateStr)
        calendar.setTime(date)

        val dayWeek = calendar.get(Calendar.DAY_OF_WEEK) match {
          case 1 => "星期日"
          case 2 => "星期一"
          case 3 => "星期二"
          case 4 => "星期三"
          case 5 => "星期四"
          case 6 => "星期五"
          case 7 => "星期六"
        }
        // 返回星期即可
        dayWeek
      }
    )

    //对数据处理，使用udf
    val reportDF = dataFrame.select(to_week(col("departure_time")).as("week"))
      .groupBy("week").count()
      .select(col("week"), col("count").as("total"))
    reportDF.printSchema()
    reportDF.show(10,truncate = false)
    reportDF.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/test?serverTimezone=UTC&characterEncoding=utf-8&useSSL=false","test.week"
      ,prop)
  }


  def main(args: Array[String]): Unit = {
    // step1、构建SparkSession实例对象（集成Hudi和HDFS）
    val spark:SparkSession = SparkUtils.createSpakSession(this.getClass,partitions = 8)
    // step2、加载Hudi表的数据，指定字段
    val hudiDF: DataFrame = readFromHudi(spark, hudiTablePath)
    hudiDF.printSchema()
    hudiDF.show(10, truncate = false)
    // 由于数据被使用多次，所以建议缓存
    hudiDF.persist(StorageLevel.MEMORY_AND_DISK)

    // step3、按照业务指标进行统计分析
    // 指标1：订单类型统计
    print("------------------------")
    //为啥打印不出结果
    reportProduct(hudiDF)
    reportPrice(hudiDF)
    reportWeek(hudiDF)
    // 当数据不在使用时，释放资源
    hudiDF.unpersist()

    // step4、应用结束，关闭资源
    spark.stop()
  }

}
