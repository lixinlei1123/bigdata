package consumeAnalysis.wash

import org.apache.spark.sql.SparkSession

import common.util._

object createConsumeAddressCode {
  //声明一个地址码的键值对
  def createDict(dictList:List[Any], dictFilePath:String)={
    var addressMap = collection.mutable.Map[String,Int]()
    //给地址编号从0开始
    var id = 0
    //向文件中写入表头
    writeInfo(dictFilePath,"code,value\n")
    for(value <- dictList){
      if(value != null){
        id += 1
        println("key:"+id+", val:"+value)
        //生成地址码值键值对（没有什么用，可以不生成该map）
        addressMap += (value.toString -> id)
        //将生成出的键值对写入文件
        writeInfo(dictFilePath,id+","+value+"\n")
      }
    }
  }

  def main(args: Array[String]): Unit = {
      //启动spark。初始化spark会话
      val spark = SparkSession.builder.appName("CreateConsumeAddressCode").master("local[3]").getOrCreate()
      //读取csv文件，生成rdd
      val rdd = spark.read.option("header","true").csv("D:/resources/student-analysis/consumeanalysis/wash/consume.csv").rdd
      //读出地址去重排序，形成一个不重复的地址列表
//      val consumeAddressCodeList = rdd.filter(delNullOrNone).map(_(2).toString)
//          .distinct().sortBy((x)=>x,true).collect().toList
//
//      spark.stop()
////      println(consumeAddressCodeList)
//      //生成地址码值表并直接写入文件
//      createDict(consumeAddressCodeList,"D:/resources/student-analysis/consumeanalysis/wash/consumeAddressCodeFile.csv")
  }
}
