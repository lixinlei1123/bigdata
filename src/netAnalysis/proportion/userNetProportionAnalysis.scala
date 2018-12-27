package netAnalysis.proportion

import common.{Constant, util}
import org.apache.spark.sql.{Row, SaveMode}

//按性别分析上网时间和排名
object userNetProportionAnalysis {

  //声明按性别统计的字段标题
  val userNetProportionTitle = "sex,onlineYearMonth,semester,sexCount,sexTotleTime,sexAverageDuringTime"
  val proportionTitleType = List("String","String","String","Int","Double","Double")
  //声明按上网时间排序的字段标题
  val userNetDuringSortTitle = "sex,onlineYearMonth,semester,duringTime,ranking"
  val duringSortTitleType = List("String","String","String","Double","Long")

  //生成要返回的数据
  def transformDataList(line:Row) = {
    //学生id
    val userId = line(0).toString
    //学生性别
    val sex = line(1).toString
    //开始上网时间
    val startTime = line(2).toString
    //结束上网时间
    val endTime = line(3).toString

    //开始上网时间转成日期格式
    val startTimeDate = util.transformStrToCalender(startTime,"yyyy/MM/dd HH:mm")
    //结束上网时间转成日期格式
    val endTimeDate = util.transformStrToCalender(endTime,"yyyy/MM/dd HH:mm")

    //上网时长
    val onlineDuration = ((endTimeDate._6 - startTimeDate._6)/3600000.0)
    //把开始时间转成字符串格式，赋予变量 在线的年月
    val onlineYearMonth = startTimeDate._1.toString + startTimeDate._2.toString

    //获得学期
    val semester = util.getSemester(userId,startTimeDate)


    //在同一时间点相同性别的人 统计上网时间
    (userId+"#"+sex+"#"+onlineYearMonth+"#"+semester,(1,onlineDuration))
  }

  def showFields(line:(String,(Int,Double))) = {

      val fields = line._1.split("#")
      val sex = fields(0)
      val onlineYearMonth = fields(1)
      val semester = fields(2)

      //同一性别总人数
      val sexCount = line._2._1
      //同一性别总上网时间
      val sexTotleTime = line._2._2
      //同一性别平均上网时间
      val sexAverageDuringTime = sexTotleTime/sexCount.toFloat

      Row(sex,onlineYearMonth,semester,sexCount,sexTotleTime,sexAverageDuringTime)
  }



  def main(args: Array[String]): Unit = {
      val spark = util.createSpark(Constant.MASTER,"userNetProportionAnalysis")
      //声明一个long型的累加器
      val sortByAccu = spark.sparkContext.longAccumulator("sortByAccu")
      val dataRdd = spark.read
          .option("header","true")
          .csv(Constant.NETPATH + "washedNetData")
          .rdd

      //对原始数据进行第一步处理，生成上网时间字段并合并。并做持久化
      val userNetListRdd = dataRdd
        .map(transformDataList)
        .reduceByKey((x,y)=>{
          (x._1+y._1,x._2+y._2)
        })
        .cache()

      //生成性别，上网年月，学期,每个性别总人数，每个性别总共上网时间，每个性别平均上网时间字段
      val sexCountRdd = userNetListRdd.map(showFields)

      //统计不同性别上网时长统计，创建相应schema 写入文件
      val proportionSchema = util.getSchema(userNetProportionTitle,proportionTitleType)
      spark.createDataFrame(sexCountRdd,proportionSchema).write
            .option("header",true)
            .mode(SaveMode.Overwrite)
            .csv(Constant.NETPATH + "userNetProportionAnalysis")


      //把上一步统计出来的数据按上网时长进行排序，并通过累加器写出排名
      val duringTimeSortRdd = userNetListRdd
          .map(line=>{
            val sex = line._1.split("#")(0)
            (line._1,(sex,line._2._2))
          })
          .sortBy(_._2,false)
          .map(line=>{
              sortByAccu.add(1L)

              val duringTime = line._2._2
              val fields = line._1.split("#")
              val sex = fields(0)
              val onlineYearMonth = fields(1)
              val semester = fields(2)
              Row(sex,onlineYearMonth,semester,duringTime,sortByAccu.value)
            })


      //写入文件
      util.writeFileByDF(spark,duringTimeSortRdd,(userNetDuringSortTitle,duringSortTitleType),
        Constant.NETPATH + "userNetDuringSortAnalysis")

      spark.stop()
    }

}
