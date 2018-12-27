package consumeAnalysis.wash

import java.text.SimpleDateFormat

import common.{Constant, util}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.io.Source

object washData {

  val consumeWashFile = "userId,sex,consumeAddress,consumeType,consumeYearMonth,makeCardDate,money"
  val consumeWashFileTitleType = List("String","String","String","String","String","String","Double")

  var imAddressCodeMap:Map[String,String] = Map()
  var imAddressTypeRelationMap:Map[String,String] = Map()

  def getAddressCodeMap() = {
    //声明一个存放地点码码值的键值对
    var addressCodeMap = Map[String,String]().toBuffer
    //获取地点码值
    val addressCode = Source.fromFile(Constant.CONSUMEPATH + "consumeAddressCodeFile.csv").getLines().toList
    for(i <- addressCode){
      if(!i.contains("value")){
        val info = i.split(",")
        addressCodeMap += (info(1) -> info(0))
      }
    }
    addressCodeMap.toMap
  }

  def getAddressTypeRelationMap() = {
    //声明一个存放地点码码值的键值对
    var addressTypeRelationMap = Map[String,String]().toBuffer
    //获取类型码值
    val addressTypeRelation = Source.fromFile(Constant.CONSUMEPATH + "consumeAddressTypeRelationFile.csv").getLines().toList
    for(i <- addressTypeRelation){
      if(!i.contains("consumeAddress")) {
        val info = i.toString.split(",")
        addressTypeRelationMap += (info(0) -> info(1))
      }
    }
    addressTypeRelationMap.toMap
  }

  //生成学号，消费地点，消费类型，消费年月，金额，办卡日期字段
  def transformDataList(x:Row) = {
    val userId = x(0).toString
    val sex = (if(x(1).toString == "男") 1 else 2).toString
    val consumeAddress = util.getValue(x(2).toString,imAddressCodeMap).toString
    val consumeType = imAddressTypeRelationMap(consumeAddress)
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val consumeYearMonth = x(4).toString.take(6)
    val consumeDate = sdf.parse(x(4).toString)
    val makeCardDate = x(5).toString
    val money = x(6).toString.toDouble
    Row(userId,sex,consumeAddress, consumeType, consumeYearMonth,makeCardDate,money)
  }

  def main(args: Array[String]): Unit = {
    //地址码表map
    imAddressCodeMap = getAddressCodeMap()
    //地址类型码表map
    imAddressTypeRelationMap = getAddressTypeRelationMap()

    //启动spark
    val spark = util.createSpark(Constant.MASTER,"washConsumeData")
    //读取消费信息数据并转成rdd
    val consumeDF = spark.read
      .option("header","true")
      .csv(Constant.CONSUMEPATH + "consume")
    consumeDF.createOrReplaceTempView("consume")

    //过滤掉脏数据(为空的,钱数不符合规律的,消费日期早于办卡日期的)，并转成rdd
    val resultRDD = spark.sql("select * from consume where userId is not null "
      + "and sex is not null and consumeAddress is not null "
      + "and consumeType is not null and consumeDate is not null "
      + "and createCardDate is not null and money is not null "
      + "and money>0.0 and consumeDate>createCardDate").rdd
    //然后把过滤后的每一行数据封装成一个Row
    .map(transformDataList)

    //写入文件
    util.writeFileByDF(spark,resultRDD,(consumeWashFile,consumeWashFileTitleType),
      Constant.CONSUMEPATH + "washedConsumeData")

    spark.stop()
  }

}
