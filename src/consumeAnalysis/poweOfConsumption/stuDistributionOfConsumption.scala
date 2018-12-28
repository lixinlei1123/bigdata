package consumeAnalysis.poweOfConsumption

import common.{Constant, util}
import org.apache.spark.sql.Row

object stuDistributionOfConsumption {

  val title = "consumeYearMonth,consumeLevel,totleStudent,totleMoney,avgMoney"
  val titleType = List("String","String","Int","Double","Double")

  def main(args: Array[String]): Unit = {
      val spark = util.createSpark("distributionOfConsumption",Constant.MASTER)
      import spark.implicits._

      val consumeDataDF = spark.read
          .option("header",true)
          .csv(Constant.CONSUMEPATH + "stuPowerOfConsumption")
      consumeDataDF.createOrReplaceTempView("consumeData")

      val filteredDataRDD = spark.sql("select consumeYearMonth,totleMoney," +
          "case when consumeNormalize>=70 then 'high' " +
          "when consumeNormalize>=40 and consumeNormalize<70 then 'middle' " +
          "else 'low' end as consumeLevel from consumeData").rdd

      val resultRDD = filteredDataRDD.map(line=>{
          val consumeYearMonth = line(0).toString
          val consumeLevel = line(2).toString
          val totleMoney = line(1).toString.toDouble
          (consumeYearMonth + "#" + consumeLevel,(1,totleMoney))
      }).reduceByKey((x,y)=>{
          (x._1+y._1,x._2+y._2)
      }).map(line=>{
          val baseInfo = line._1.split("#")
          val countInfo = line._2
          val consumeYearMonth = baseInfo(0)
          val consumeLevel = baseInfo(1)
          val totleStudent = countInfo._1
          val totleMoney = countInfo._2
          val avgMoney = totleMoney/totleStudent
          Row(consumeYearMonth,consumeLevel,totleStudent,totleMoney,avgMoney)
      })

      util.writeFileByRDD(spark,resultRDD,(title,titleType),
        Constant.CONSUMEPATH + "stuDistributionOfConsumption")

      spark.stop()
  }

}
