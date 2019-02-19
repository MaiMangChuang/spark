package traffic;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import util.JdbcUtils;
import util.TestDataUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

/**
 * 同行车辆分析算法实现
 * brave
 */
public class TxcCompute {
    public static void main(String[] args){
        SparkSession spark=SparkSession.builder().enableHiveSupport().appName("TxcCompute").master("local").getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");

        Dataset<Row> cltgxxDF = spark.sql("select * from t_cltgxx");
        cltgxxDF.show();
        JavaRDD<Row> cltgxxRDD = cltgxxDF.javaRDD();
        JavaPairRDD<String, Tuple2> cltgxxPairRDD = cltgxxRDD.mapToPair(new PairFunction<Row, String, Tuple2>() {
            @Override
            public Tuple2<String, Tuple2> call(Row row) throws Exception {
                String hphm = row.getAs("hphm");
                String tgsj = ((Timestamp) row.getAs("tgsj")).toString();
                String kkbh = row.getAs("kkbh");
                return new Tuple2<String, Tuple2>(hphm, new Tuple2<String, String>(tgsj, kkbh));
            }
        });//(hphm,(tgsj,kkbh))

        //groupByKey
        JavaPairRDD<String, Iterable<Tuple2>> cltgxxGroupByKey = cltgxxPairRDD.groupByKey();

        JavaRDD<Tuple2<String, Tuple2<String,String>>> tuple2RDD = cltgxxGroupByKey.flatMap(new FlatMapFunction<Tuple2<String, Iterable<Tuple2>>, Tuple2<String, Tuple2<String,String>>>() {
            @Override
            public Iterator<Tuple2<String, Tuple2<String,String>>> call(Tuple2<String, Iterable<Tuple2>> inIterTuple2) throws Exception {
                List<Tuple2<String, Tuple2<String,String>>> result = new ArrayList<Tuple2<String, Tuple2<String,String>>>();
                //iterable转List
                List<Tuple2> list = IteratorUtils.toList(inIterTuple2._2.iterator());
                for (int i = 0; i < list.size() - 2; i++) {
                    StringBuilder sbTime = new StringBuilder();
                    StringBuilder sbKkbh = new StringBuilder();
                    for (int j = 0; j < 3; j++) {
                        if ((j + i) < list.size()) {
                            sbTime.append(list.get(j + i)._1).append(",");
                            sbKkbh.append(list.get(j + i)._2).append(",");
                        } else {
                            break;
                        }
                    }
                    System.out.println("sbTime:" + sbTime.toString());
                    System.out.println("sbKkbh:" + sbKkbh.toString());
                    //(kkbh ,(hphm,Time))
                    result.add(new Tuple2<String, Tuple2<String,String>>(sbKkbh.toString(), new Tuple2(inIterTuple2._1, sbTime.toString())));
                }
                return result.iterator();
            }
        });
        tuple2RDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String,String>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String,String>> t2) throws Exception {
                System.out.println(t2._1+":("+t2._2._1+",["+t2._2._2+"])");
            }
        });
        /**
         * kkbh1,kkbh2,kkbh3,:(hphm1,[t1,t2,t3,])
         * kkbh2,kkbh3,kkbh4,:(hphm1,[t2,t3,t4,])
         * kkbh3,kkbh4,kkbh5,:(hphm1,[t3,t4,t5,])
         * tuple2RDD是一个Tuple2类型的RDD，不是一个PairRDD
         */
        tuple2RDD.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>, String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, Tuple2<String,String>> call(Tuple2<String, Tuple2<String,String>> inT2) throws Exception {
                return new Tuple2<String, Tuple2<String,String>>(inT2._1,inT2._2);
            }
        }).groupByKey().map(new Function<Tuple2<String,Iterable<Tuple2<String,String>>>, String>() {
            @Override
            public String call(Tuple2<String, Iterable<Tuple2<String, String>>> inT2) throws Exception {

                Set<String> hphmSet=new HashSet<String>();
                StringBuilder sbHphm=new StringBuilder();
                //kkbh
                String kkbh=inT2._1;
                System.out.println("kkbh****: "+kkbh);
                //(hphm1,[t1,t2,t3,])
                List<Tuple2<String,String>> list = IteratorUtils.toList(inT2._2.iterator());
                //list元素两两比较
                for(int i=0;i<list.size();i++){
                    for(int j=i+1;j<list.size();j++){
                        String times1=list.get(i)._2;
                        String times2=list.get(j)._2;
                        String hphm1 = list.get(i)._1;
                        String hphm2 = list.get(j)._1;
                        System.out.println("hphm1****: "+ hphm1);
                        System.out.println("hphm2****: "+ hphm2);
                        System.out.println("time1****: "+times1);
                        System.out.println("time2****: "+times2);
                        String[] timeArray1 = times1.split(",");
                        String[] timeArray2 = times2.split(",");
                        for(int k=0;k<timeArray1.length;k++){
                            double subMinutes = TestDataUtil.getSubMinutes(timeArray1[i], timeArray2[i]);
                            if(subMinutes<=3){
                                hphmSet.add(hphm1);
                                hphmSet.add(hphm2);
                            }
                        }
                    }
                }
                for(String hphm : hphmSet){
                    sbHphm.append(hphm).append(",");
                }
                String resultStr=kkbh+"&"+(sbHphm.toString());
                return resultStr;
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.split("&").length>1;
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                String hphm = s.split("&")[1];
                String kkbh = s.split("&")[0];
                System.out.println(kkbh +" ："+ hphm);
                Connection conn =JdbcUtils.getConnection();
                PreparedStatement pstmt= null;
                String sql = "insert into t_txc_result (JSBH,HPHM,KKBH,CREATE_TIME) values(?,?,?,?)";
                pstmt = conn.prepareStatement(sql);
                long jsbh=System.currentTimeMillis();
                pstmt.setString(1,jsbh+"");
                pstmt.setString(2,hphm);
                pstmt.setString(3,kkbh);
                pstmt.setTimestamp(4,new Timestamp(jsbh));
                pstmt.executeUpdate();
                JdbcUtils.free(pstmt, conn);
            }
        });
    }
}
