package netAnalysis.proportion

import common.{Constant, util}
import org.apache.spark.sql.Row


object userNetProportionAnalysis {

  //声明文件的字段标题
  val userNetProportionTitle = "userId,sex,onlineYearMonth,semester,userNetDegree"

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
    //学生入学年份
    val year = userId.take(4).toInt
    //上网的年份
    val onlineYear = startTimeDate._1
    //上网的月份
    val onlineMonth = startTimeDate._2

    //通过年份和月份计算出学期
    var semester = (onlineYear - year) * 2
    if (onlineMonth > 6) {
      semester += 1
    }

    (userId+"#"+sex+"#"+onlineYearMonth+"#"+semester,onlineDuration.toInt)
  }



  def main(args: Array[String]): Unit = {
      val spark = util.createSpark(Constant.MASTER,"userNetProportionAnalysis")
      val dataRdd = spark.read
          .option("header","true")
          .csv(Constant.NETPATH + "washedNetData")
          .rdd

      //生成学号，性别，上网年月，学期,熬夜次数，上网时长字段
//      val userNetList = dataRdd
//          .map(transformDataList)
//          .reduceByKey{(x,y)=>
//            (x._1 + y._1,x._2 + y._2)
//          }
  }

}
