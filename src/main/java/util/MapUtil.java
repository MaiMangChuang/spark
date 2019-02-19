package util;

import java.math.BigDecimal;
import java.util.Random;

/**
 * 地图相关的工具类，用于生成经纬度数据和计算地图上两点之间的距离。
 * @author brave
 */
public class MapUtil {
    static double DEF_PI = 3.14159265359; // PI
    static double DEF_2PI= 6.28318530712; // 2*PI
    static double DEF_PI180= 0.01745329252; // PI/180.0
    static double DEF_R =6370693.5; // radius of earth
    //适用于近距离
    public static double getShortDistance(double lon1, double lat1, double lon2, double lat2)
    {
        double ew1, ns1, ew2, ns2;
        double dx, dy, dew;
        double distance;
        // 角度转换为弧度
        ew1 = lon1 * DEF_PI180;
        ns1 = lat1 * DEF_PI180;
        ew2 = lon2 * DEF_PI180;
        ns2 = lat2 * DEF_PI180;
        // 经度差
        dew = ew1 - ew2;
        // 若跨东经和西经180 度，进行调整
        if (dew > DEF_PI)
        dew = DEF_2PI - dew;
        else if (dew < -DEF_PI)
        dew = DEF_2PI + dew;
        dx = DEF_R * Math.cos(ns1) * dew; // 东西方向长度(在纬度圈上的投影长度)
        dy = DEF_R * (ns1 - ns2); // 南北方向长度(在经度圈上的投影长度)
        // 勾股定理求斜边长
        distance = Math.sqrt(dx * dx + dy * dy);
        return distance;
    }
    //适用于远距离
    public static double getLongDistance(double lon1, double lat1, double lon2, double lat2)
    {
        double ew1, ns1, ew2, ns2;
        double distance;
        // 角度转换为弧度
        ew1 = lon1 * DEF_PI180;
        ns1 = lat1 * DEF_PI180;
        ew2 = lon2 * DEF_PI180;
        ns2 = lat2 * DEF_PI180;
        // 求大圆劣弧与球心所夹的角(弧度)
        distance = Math.sin(ns1) * Math.sin(ns2) + Math.cos(ns1) * Math.cos(ns2) * Math.cos(ew1 - ew2);
        // 调整到[-1..1]范围内，避免溢出
        if (distance > 1.0)
             distance = 1.0;
        else if (distance < -1.0)
              distance = -1.0;
        // 求大圆劣弧长度
        distance = DEF_R * Math.acos(distance);
        //四舍五入：单位KM
        return Math.round(distance/1000);
    }

    /**
     * @Title: 随机经纬度
     * @Description: 在矩形内随机生成经纬度
     * @param MinLon：最小经度  MaxLon： 最大经度   MinLat：最小纬度   MaxLat：最大纬度  
     * MinLon:111.177463 MaxLon 117.101392
     * MinLat:22.569001  MaxLat 25.558735
     * @return
     * @throws
     */
   public static String randomLonLat(double MinLon, 
				   					  double MaxLon, 
				   					  double MinLat, 
				   					  double MaxLat) {
     Random random = new Random();
     BigDecimal db = new BigDecimal(Math.random() * (MaxLon - MinLon) + MinLon);
     String lon = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();// 小数后6位
     db = new BigDecimal(Math.random() * (MaxLat - MinLat) + MinLat);
     String lat = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();
     return lon+"_"+lat;
   }

    public static void main(String[] args) {
        //113.286789,23.148599
        double mLat1 = 33.670271; // point1纬度
        double mLon1 = 114.035958; // point1经度
        double mLat2 = 35.553931;// point2纬度
        double mLon2 = 114.035958;// point2经度
        double distance = MapUtil.getLongDistance(mLon1, mLat1, mLon2, mLat2);
        System.out.println(distance);
        /**
         * 随机经纬度
         */
//        for(int i=0;i<10;i++){
//        	String lonLat=randomLonLat(111.177463, 117.101392, 22.569001, 25.558735);
//        	System.out.println(lonLat);
//        }
    }
}