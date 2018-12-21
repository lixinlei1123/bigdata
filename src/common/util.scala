package common

import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

//分析每个小时的上网次数
object util {

  //创建sparkSession(other做预留其他特殊情况)
  def createSpark(appName:String,master:String,other:(String,String)*) = {
    SparkSession.builder()
      .master(Constant.MASTER)
      .appName("washNetData")
      .getOrCreate()
  }

  //封装一个类型的map
  val typeMap = Map(
    ("String",StringType),
    ("Int",IntegerType),
    ("Double",DoubleType),
    ("Float",FloatType),
    ("Long",LongType)
  )
  //封装schema
  def getSchema(args:String,titleType:List[String]=null) = {
      var fields:Array[StructField] = null
      if(null == titleType && titleType.length == 0){
        fields = args.split(",").map(arg=>{
          StructField(arg, StringType, nullable = true)
        })
      }else{
        val titleAndType = args.split(",").toList.zip(titleType).toArray
        fields = titleAndType.map(arg=>{
          StructField(arg._1, typeMap(arg._2), nullable = true)
        })
      }
      StructType(fields)
  }

  //把字符串类型日期转成calender类型
  def transformStrToCalender(date:String,format:String) = {
      val calender = Calendar.getInstance()
      val sdf = new SimpleDateFormat(format)
      calender.setTime(sdf.parse(date))
      val year = calender.get(Calendar.YEAR)
      val month = calender.get(Calendar.MONTH)+1
      val day = calender.get(Calendar.DATE)
      val hour = calender.get(Calendar.HOUR)
      val minute = calender.get(Calendar.MINUTE)
      val time = calender.getTime.getTime
      (year,month,day,hour,minute,time)
  }

  def getSemester(userId:String,timeDate:(Int,Int,Int,Int,Int,Long)) = {
    //学生入学年份
    val year = userId.take(4).toInt
    //上网的年份
    val onlineYear = timeDate._1
    //上网的月份
    val onlineMonth = timeDate._2

    //通过年份和月份计算出学期
    var semester = (onlineYear - year) * 2
    if (onlineMonth > 6) {
      semester += 1
    }
  }


  //能力值算法
  def normalize(minVal:Int,maxVal:Int,x:Int) : Float = {
    if((maxVal.asInstanceOf[Float]-minVal.asInstanceOf[Float]) == 0.0){
      0.toFloat
    }else{
      ((x.asInstanceOf[Float]-minVal.asInstanceOf[Float]) / (maxVal.asInstanceOf[Float]-minVal.asInstanceOf[Float])).toFloat
    }
  }

  //把清洗后的数据写入文件
  def writeInfo(fileName:String,str:String) = {
    val file = new File(fileName)
    val fw = new FileWriter(file,true)
    fw.write(str)
    fw.close()
  }

  //格式化日期，可能用不到。。。
  def formatDate(unFormatDate:String) = {
    val dateAndTime = unFormatDate.split(" ")
    val date = dateAndTime(0)
    val YMD = date.split("/")
    val year = YMD(0)
    var month = YMD(1)
    var day = YMD(2)

    if(month.length != 2){
      month = "0"+month
    }
    if(day.length != 2){
      day = "0"+day
    }

    val time = dateAndTime(1)
    val HMS = time.split(":")
    var hour = HMS(0)
    var minute = HMS(1)

    if(hour.length != 2){
      hour = "0"+hour
    }
    if(minute.length != 2){
      minute = "0"+minute
    }

    year+"/"+month+"/"+day+" "+hour+":"+minute
  }
}
