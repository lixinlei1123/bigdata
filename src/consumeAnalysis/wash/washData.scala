package consumeAnalysis.wash

import java.text.SimpleDateFormat

import org.apache.spark.sql.{Row, SparkSession}
import common.util._

import scala.io.Source

object washData {

  var imAddressCodeMap:Map[String,String] = Map()
  var imAddressTypeRelationMap:Map[String,String] = Map()


  def getId(address:String,addressMap:Map[String,String]):String = {
    if (address == null || address == "") {
      "0"
    }else{
      addressMap(address)
    }
  }
  //生成学号，消费地点，消费类型，消费年月，金额，办卡日期字段
  def transformDataList(x:Row) = {
    val userId = x(0)
    val consumeAddress = getId(x(2).toString,imAddressCodeMap)
    val consumeType = imAddressTypeRelationMap(consumeAddress)

    val sdf = new SimpleDateFormat("yyyyMMdd")
    val consumeDate = sdf.parse(x(4).toString)
    val consumeYearMonth = x(4).toString.take(6)+"01"
    val money = x(6)
    val makeCardDate = x(5)
    List(userId, consumeAddress, consumeType, consumeYearMonth, money, makeCardDate)
  }

  def main(args: Array[String]): Unit = {
    //获取地点码值
    val addressCode = Source.fromFile("D:/resources/student-analysis/consumeanalysis/wash/consumeAddressCodeFile.csv").getLines().toList
    //声明一个存放地点码码值的键值对
    var addressCodeMap = collection.mutable.Map[String,String]()
    for(i <- addressCode){
      if(!i.contains("value")){
        val info = i.split(",").toList
//        println(info(1)+"==================="+info(0))
        addressCodeMap += (info(1) -> info(0))
      }
    }
    imAddressCodeMap = addressCodeMap.toMap

    //获取类型码值
    val addressTypeRelation = Source.fromFile("D:/resources/student-analysis/consumeanalysis/wash/consumeAddressTypeRelationFile.csv").getLines().toList
    //声明一个存放地点码码值的键值对
    var addressTypeRelationMap = collection.mutable.Map[String,String]()
    for(i <- addressTypeRelation){
      if(!i.contains("consumeAddress")) {
        val info = i.toString.split(",").toList
        addressTypeRelationMap += (info(0) -> info(1))
      }
    }
    imAddressTypeRelationMap = addressTypeRelationMap.toMap
//    println(imAddressTypeRelationMap)

    //启动spark
    val spark = SparkSession.builder.appName("WashConsumeData").master("local[3]").getOrCreate()
    //读取消费信息数据并转成rdd
    val consumeRdd = spark.read.option("header","true").csv("D:/resources/student-analysis/consumeanalysis/wash/consume.csv").rdd
    //生成学号，消费地点，消费类型，消费年月，金额，办卡日期字段
//    val lines = consumeRdd.filter(delNullOrNone).map(transformDataList).collect()
//
//    for(i <- lines) {
//      writeInfo("D:/resources/student-analysis/consumeanalysis/wash/WashConsumeData.csv", i.mkString(",") + "\n")
//    }
  }

}
