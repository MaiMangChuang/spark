package traffic.utilscala

import util.{MapUtil, TestDataUtil}

object ComputeUDF {
  def getTpc(s: String): String = {
    val result = new StringBuilder
    //"ID+"_"+lon+"_"+lat+"_"+tgsj & ID+"_"+lon+"_"+lat+"_"+tgsj"
    val value = s
    //[ID+"_"+lon+"_"+lat+"_"+tgsj]
    val values = value.split("&")
    //遍历values 进行业务逻辑处理-------开发人员编写业务逻辑处理代码
    var i = 0
    while (i < values.length) {
      var k = i + 1
      while (k < values.length) {
        val value1 = values(i)
        val value2 = values(k)
        val items1 = value1.split("_")
        val items2 = value2.split("_")
        val id1 = items1(0)
        val lon1 = items1(1).toDouble
        val lat1 = items1(2).toDouble
        val tgsj1 = items1(3)
        val id2 = items2(0)
        val lon2 = items2(1).toDouble
        val lat2 = items2(2).toDouble
        val tgsj2 = items2(3)
        val subHour = TestDataUtil.getSubHour(tgsj1, tgsj2)
        val distance = MapUtil.getLongDistance(lon1, lat1, lon2, lat2)
        val speed = TestDataUtil.getSpeed(distance, subHour)
        if (speed > 120) { //如果车牌号相同的两车,
          // 卡口时间相差值，卡口距离值====>速度大于120km/h，则为套牌车。或者
          // 卡口时间相差小于等于5分钟，同时两车卡口距离大于10KM(即速度大于120km/h)，则为套牌车
          if (result.length > 0) result.append("&").append(id1 + "_" + id2) //符合条件
          else result.append(id1 + "_" + id2)
        }
        k += 1;
        k - 1
      }
      i += 1;
      i - 1
    }

    return if (result.toString.length > 0) result.toString
    else null
  }


}
