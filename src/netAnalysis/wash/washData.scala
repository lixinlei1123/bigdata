package netAnalysis.wash

import common.{Constant, util}
import org.apache.spark.sql.{Row, SaveMode}

import scala.io.Source

object washData {

  //写入文件的标题
  val washNetDataTitle = "userId,sex,startTime,endTime"

  //从性别表中读数据封装成map
  def createSexMap() = {
    //从性别表中读数据
    val sexList = Source.fromFile(Constant.NETPATH + "sexDictFile.csv").getLines().toList
    //声明一个存放学号和性别的map
    var sexMap = Map[String,String]().toBuffer
    for(i <- sexList){
      val info = i.toString.split(",").toList
      sexMap += (info(0) -> info(1))
    }
    //返回封装好的不可变性别map
    sexMap.toMap
  }

  def main(args: Array[String]): Unit = {
    //取性别map，下面会用到
    val sexMap = createSexMap()

    //创建sparkSession
    val spark = util.createSpark(Constant.MASTER,"washNetData")

    //读取netClean源文件数据生成DataFrame
    val netDF = spark.read.option("header","true").csv(Constant.NETPATH + "netClean")
    //将读到的数据创建成临时表
    netDF.createOrReplaceTempView("net")
    //通过sql语句过滤脏数据，并把过滤之后的数据转成rdd计算
    val filterNetRdd = spark.sql("select userId,startTime,endTime" +
              " from net where startTime<endTime and userId is not null " +
              "and startTime is not null and endTime is not null").rdd
    //把rdd里的每行数据转成Row类型
    val result = filterNetRdd
      .map(line =>
          Row(line(0),sexMap(line(0).toString),line(1),line(2))
      )
    //调用封装好的方法创建schema
    val schema = util.getSchema(washNetDataTitle)
    //创建最终DataFrame写入文件
    spark.createDataFrame(result,schema).write
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .csv(Constant.NETPATH + "washedNetData")

    spark.stop()
  }

}
