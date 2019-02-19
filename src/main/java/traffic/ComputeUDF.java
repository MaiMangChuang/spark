package traffic;

import org.apache.spark.sql.api.java.UDF1;
import util.MapUtil;
import util.TestDataUtil;

public class ComputeUDF implements UDF1<String, String> {
    @Override
    public String call(String s) throws Exception {
        StringBuilder result=new StringBuilder();
        //"ID+"_"+lon+"_"+lat+"_"+tgsj & ID+"_"+lon+"_"+lat+"_"+tgsj"
        String value=s;
        //[ID+"_"+lon+"_"+lat+"_"+tgsj]
        String[] values = value.split("&");
        //遍历values 进行业务逻辑处理-------开发人员编写业务逻辑处理代码
        for(int i=0;i<values.length;i++){
            for(int k=i+1;k<values.length;k++){
                String value1=values[i];
                String value2=values[k];
                String[] items1=value1.split("_");
                String[] items2=value2.split("_");
                String id1=items1[0];
                String lon1=items1[1];
                String lat1=items1[2];
                String tgsj1=items1[3];

                String id2=items2[0];
                String lon2=items2[1];
                String lat2=items2[2];
                String tgsj2=items2[3];

                double subHour= TestDataUtil.getSubHour(tgsj1, tgsj2);
                double distance= MapUtil.getLongDistance(Double.valueOf(lon1), Double.valueOf(lat1),Double.valueOf(lon2),Double.valueOf(lat2));
                Integer speed = TestDataUtil.getSpeed(distance, subHour);
                if(speed>180){
                    //如果车牌号相同的两车,
                    // 卡口时间相差值，卡口距离值====>速度大于180km/h，则为套牌车。或者
                    // 卡口时间相差小于等于5分钟，同时两车卡口距离大于10KM(即速度大于120km/h)，则为套牌车
                    if(result.length()>0){
                        result.append("&").append(id1+"_"+id2);//符合条件
                    }
                    else
                    {
                        result.append(id1+"_"+id2);//符合条件
                    }

                }
            }
        }

        return result.toString().length()>0?result.toString(): null;
    }
}
