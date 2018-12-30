package consumeAnalysis.poweOfConsumption

import common.{Constant, util}
import org.apache.spark.sql.{Row, SaveMode}

object stuPowerOfConsumption {

    val stuConsumeTypeTitle = "userId,sex,consumeYearMonth,consumeCount," +
                              "canteenTotleMoney,superMarketTotleMoney," +
                              "hospitalTotleMoney,showerRoomTotleMoney," +
                              "totleMoney,avgMoney,consumeNormalize,rank"
    val stuConsumeTypeTitleType = List("String","String","String","Int","Double","Double","Double","Double","Double","Double","Double","Long")

    //食堂消费最大值
    var maxCanteenConsume:Int = 0
    //食堂消费最小值
    var minCanteenConsume:Int = 0
    //超市消费最大值
    var maxSupermarketConsume:Int = 0
    //超市消费最小值
    var minSupermarketConsume:Int = 0

    //把每一行数据封装成tuple，方便下面做reduceByKey
    def getLineData(line:Row) = {
          val userId = line(0).toString
          val sex = line(1).toString
          val consumeYearMonth = line(3).toString
          val consumeCount = line(4).toString.toInt
          val canteenTotleMoney = line(5).toString.toDouble
          val superMarketTotleMoney = line(6).toString.toDouble
          val hospitalTotleMoney = line(7).toString.toDouble
          val showerRoomTotleMoney = line(8).toString.toDouble

          (userId+"#"+sex+"#"+consumeYearMonth,(consumeCount,canteenTotleMoney,superMarketTotleMoney,hospitalTotleMoney,showerRoomTotleMoney))

    }

    def main(args: Array[String]): Unit = {
        val spark = util.createSpark("stuPowerOfConsumption",Constant.MASTER)
        import spark.implicits._

        //声明一个命名的累加器
        val consumeAccu = spark.sparkContext.longAccumulator("consumeAccu")
        //读取原数据
        val consumeDataDF = util.readCSVFile(spark,Constant.CONSUMEPATH + "washedConsumeData")
        //创建临时表
        consumeDataDF.createOrReplaceTempView("washedConsume")
        //食堂消费统计DataFrame
        val canteeDF = spark.sql("select userId,first(sex) as sex,first(consumeType) as consumeType," +
            "first(consumeYearMonth) as consumeYearMonth," +
            "count(*) as consumeCount,sum(money) as canteenTotleMoney,0 as superMarketTotleMoney, " +
            " 0 as hospitalTotleMoney,0 as showerRoomTotleMoney " +
            " from washedConsume where consumeType='1' group by userId")
        //超市消费统计DataFrame
        val superMarketDF = spark.sql("select userId,first(sex) as sex,first(consumeType) as consumeType," +
          "first(consumeYearMonth) as consumeYearMonth," +
          "count(*) as consumeCount,0 as canteenTotleMoney,sum(money) as superMarketTotleMoney, " +
          " 0 as hospitalTotleMoney,0 as showerRoomTotleMoney " +
          " from washedConsume where consumeType='2' group by userId")
        //医院消费统计DataFrame
        val hospitaltDF = spark.sql("select userId,first(sex) as sex,first(consumeType) as consumeType," +
          "first(consumeYearMonth) as consumeYearMonth," +
          "count(*) as consumeCount,0 as canteenTotleMoney,0 as superMarketTotleMoney, " +
          " sum(money) as hospitalTotleMoney,0 as showerRoomTotleMoney " +
          " from washedConsume where consumeType='3' group by userId")
        //浴室消费统计DataFrame
        val showerRoomDF = spark.sql("select userId,first(sex) as sex,first(consumeType) as consumeType," +
          "first(consumeYearMonth) as consumeYearMonth," +
          "count(*) as consumeCount,0 as canteenTotleMoney,0 as superMarketTotleMoney, " +
          " 0 as hospitalTotleMoney,sum(money) as showerRoomTotleMoney " +
          " from washedConsume where consumeType='4' group by userId")
        //把所有消费类型数据整合
        val consumeDataAllDF = canteeDF.union(superMarketDF).union(hospitaltDF).union(showerRoomDF).coalesce(1)
        //把当前按消费类型和学生id统计的数据写入文件，方便其他功能使用
        util.writeFileByDF(consumeDataAllDF,Constant.CONSUMEPATH + "stuFilteredConsumeData")
        //通过reduceByKey算出每个学生的消费总次数，4种消费类型分别的总金额
        val stuConsumeTypeRDD = consumeDataAllDF.rdd.map(getLineData)
            .reduceByKey((x,y)=>{
                (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,x._5+y._5)
            })

        //分别求出食堂消费最大值，食堂消费最小值，超市消费最大值，超市消费最小值
        maxCanteenConsume = stuConsumeTypeRDD.map(_._2._2).max().toInt
        minCanteenConsume = stuConsumeTypeRDD.map(_._2._2).min().toInt
        maxSupermarketConsume = stuConsumeTypeRDD.map(_._2._3).max().toInt
        minSupermarketConsume = stuConsumeTypeRDD.map(_._2._3).min().toInt

        //求出消费总金额，平均消费额，食堂消费能力值，超市消费能力值，消费能力值（根据食堂和超市能力值）
        val resultRDD = stuConsumeTypeRDD.map(line=>{
              val fields = line._1.split("#")
              val money = line._2
              val totleMoney = money._2+money._3+money._4+money._5
              val avgMoney = totleMoney/money._1
              val canteenNor = util.normalize(minCanteenConsume,maxCanteenConsume,money._2.toInt)
              val superMarketNor = util.normalize(minSupermarketConsume,maxSupermarketConsume,money._3.toInt)
              val consumeNormalize = 100 * (canteenNor * 0.7 + superMarketNor * 0.3)
              List(fields(0),fields(1),fields(2),money._1,money._2,money._3,money._4,money._5,totleMoney,avgMoney,consumeNormalize)
        })
          //按照能力值排序
          .sortBy(line=>line(10).toString.toDouble,false)
          //在上一步数据基础上添加排名
          .map(line=>{
              consumeAccu.add(1)
              val seq = List.concat(line,List(consumeAccu.value)).toSeq
              Row.fromSeq(seq)
            })
          //把分区强制改为1(可改可不改)
//          .coalesce(1)

        //写入文件
        util.writeFileByRDD(spark,resultRDD,(stuConsumeTypeTitle,stuConsumeTypeTitleType)
          ,Constant.CONSUMEPATH + "stuPowerOfConsumption")

        spark.stop()
    }

}
