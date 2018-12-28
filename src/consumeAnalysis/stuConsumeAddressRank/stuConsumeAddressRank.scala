package consumeAnalysis.stuConsumeAddressRank

import common.{Constant, util}
import org.apache.spark.sql.Row

object stuConsumeAddressRank {

  val title = "consumeType,consumeYearMonth,consumeCountByType,countProportion,totleMoneyByType,moneyProportion,rank"
  val titleType = List("String","String","Int","String","Double","String","Long")

  //总消费次数
  var countTotle = 0.0
  //总消费金额
  var moneyTotle = 0.0

  //把每一行数据封装成tuple，方便下面做reduceByKey
  def getConsumeCountData(line:Row) = {
    val userId = line(0).toString
    val sex = line(1).toString
    val consumeType = line(2).toString
    val consumeYearMonth = line(3).toString
    val consumeCountByType = line(4).toString.toInt
    val canteenTotleMoney = line(5).toString.toDouble
    val superMarketTotleMoney = line(6).toString.toDouble
    val hospitalTotleMoney = line(7).toString.toDouble
    val showerRoomTotleMoney = line(8).toString.toDouble
    val totleMoneyByType = canteenTotleMoney + superMarketTotleMoney + hospitalTotleMoney + showerRoomTotleMoney

    (consumeType + "#" + consumeYearMonth, (consumeCountByType, totleMoneyByType))
  }

  def main(args: Array[String]): Unit = {
      val spark = util.createSpark("consumeAddressRank",Constant.MASTER)
      //创建累加器，下面做消费排名用
      val consumeTypeRankAccu = spark.sparkContext.longAccumulator("consumeTypeRank")
      //读取原数据
      val filteredConsumeDataDF = spark.read
        .option("header",true)
        .csv(Constant.CONSUMEPATH + "stuFilteredConsumeData")

      //下面多次使用到当前DF，持久化
      filteredConsumeDataDF.cache()

      //用sql语句查找出总消费次数和消费总金额
      filteredConsumeDataDF.createOrReplaceTempView("consumeView")
      spark.sql("select sum(consumeCount) as consumeCount," +
          "sum(canteenTotleMoney)+sum(superMarketTotleMoney)+" +
          "sum(hospitalTotleMoney)+sum(showerRoomTotleMoney) as totleMoney " +
          "from consumeView")
      .rdd
      .map(line=>{
        countTotle = line(0).toString.toDouble
        moneyTotle = line(1).toString.toDouble
      }).collect()

      //按消费类别求出每个类别分别的消费次数和总金额，并排序，用累加器写入排名
      val resRDD = filteredConsumeDataDF.rdd
          .map(getConsumeCountData)
          .reduceByKey((x,y)=>{
            (x._1+y._1,x._2+y._2)
          }).map(line=>{
          val typeAndYearMonth = line._1.split("#")
          val consumeType = typeAndYearMonth(0)
          val consumeYearMonth = typeAndYearMonth(1)
          val countByType = line._2._1
          val moneyByType = line._2._2
          Row(consumeType,consumeYearMonth,countByType,(countByType/countTotle) * 100+"%",moneyByType,(moneyByType/moneyTotle) * 100+"%")
      }).sortBy(_(4).toString.toDouble,false,1)
        .map(line=>{
          consumeTypeRankAccu.add(1)
          Row.merge(line,Row(consumeTypeRankAccu.value.toLong))
        })

      //将得到的数据写入到文件
      util.writeFileByRDD(spark,resRDD,(title,titleType),Constant.CONSUMEPATH + "stuConsumeAddressRank")
      spark.stop()
  }

}
