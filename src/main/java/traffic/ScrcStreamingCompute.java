package traffic;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import util.JdbcUtils;

import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 首次入城车辆布控算法实现
 * brave
 */

public class ScrcStreamingCompute {
    public static void main(String[] args) throws InterruptedException {
        SparkSession spark=SparkSession.builder().enableHiveSupport().appName("ScrcStreamingCompute").master("local[2]").getOrCreate();
        JavaSparkContext sc=new JavaSparkContext(spark.sparkContext());
        sc.setLogLevel("ERROR");
        //获取一定时间内的过车数据。注意：只加载需要的字段，提高性能。
        Dataset<Row> cltgxxDF = spark.sql("select hphm from t_cltgxx t where t.tgsj>'2017-03-24 09:22:22'");
        cltgxxDF.show();
        JavaRDD<Row> cltgxxRDD = cltgxxDF.javaRDD();
        JavaPairRDD<String, String> cltgxxPairRDD = cltgxxRDD.mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String hphm = row.getAs("hphm");
                return new Tuple2<String, String>(hphm,hphm);
            }
        });
        spark.sparkContext().setLogLevel("ERROR");
        JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
        String brokers = "bigdata01:9092";
        String topics = "cnwTopic";

        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(6));

        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        //kafka相关参数
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "scrc_group");
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Topic分区
        Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();
//        offsets.put(new TopicPartition("topic1", 0), 2L);
        /**
         * KafkaUtils老版本的用法，参见spark软件包中的example目录中的相关例子。
         * 本项目采用新版本的用法。
         * 通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定。
         * LocationStrategies(本地策略)
         * PreferConsistent方法：将分区数据尽可能均匀地分配给所有可用的Executor(绝大多数情况使用此方法)。
         * PreferBrokers方法：如果Executor和kafka broker在同一台机器上，用此方法，将优先将分区调度到kafka分区leader所在的主机上。
         * PreferFixed方法：分区之间的负荷有明显的倾斜，用此方法。允许指定一个明确的分区到主机的映射。
         *ConsumerStrategies(消费者策略)
         * 新的Kafka消费者API有许多不同的方式来指定主题。它们相当多的是在对象实例化后进行设置的。
         * ConsumerStrategies提供了一种抽象，即使spark任务重启，也能获得配置好的消费者信息。
         * ConsumerStrategies的Subscribe方法通过一个确定的集合来指定Topic
         * ConsumerStrategies的SubscribePattern方法允许你使用正则表达式来指定Topic。
         *
         */
        JavaInputDStream<ConsumerRecord<String,String>> lines = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams, offsets)
        );
        //需要注意:lines里的数据是个ConsumerRecord对象
        JavaPairDStream<String, String> pairDStream = lines.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> message) throws Exception {
                List<String> result = new ArrayList<String>();
                String messageValue = message.value();//json数组
                Gson gson = new Gson();
                Type type = new TypeToken<List<Map<String, String>>>() {
                }.getType();
                List<Map<String, String>> listMap = gson.fromJson(messageValue, type);
                for (Map<String, String> map : listMap) {
                    StringBuilder sbTemp = new StringBuilder();
                    sbTemp.append(map.get("HPHM")).append("_");
                    sbTemp.append(map.get("CLPP")).append(",");
                    sbTemp.append(map.get("CLYS")).append(",");
                    sbTemp.append(map.get("TGSJ")).append(",");
                    sbTemp.append(map.get("KKBH")).append(",");
                    result.add(sbTemp.toString());
                }
                return result.iterator();
            }
        }).mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split("_")[0], s.split("_")[1]);
            }
        });
        pairDStream.print();
        JavaDStream<String> resultDStream = pairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> pairRDD) throws Exception {
                JavaRDD<String> resultRDD =pairRDD.leftOuterJoin(cltgxxPairRDD).filter(new Function<Tuple2<String, Tuple2<String, Optional<String>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<String>>> t2) throws Exception {
                        Optional<String> opt = t2._2._2;
                        //如果实时入城卡口过车数据中的hphm在t_cltgxx中不存在，则为首次入城车辆！
                        return !opt.isPresent();
                    }
                }).map(new Function<Tuple2<String,Tuple2<String,Optional<String>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<String>>> t2) throws Exception {
                        System.out.println("t2._1******="+t2._1);
                        System.out.println("t2._2._1******="+t2._2._1);
                        System.out.println("t2._2._2******="+t2._2._2);
                        return t2._1+","+t2._2._1;
                    }
                });
                return resultRDD;
            }
        });
        resultDStream.print();
        resultDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> iter) throws Exception {
                        Connection conn = JdbcUtils.getConnection();
                        PreparedStatement pstmt= null;
                        while(iter.hasNext()){
                            String data = iter.next();
                            String[] fields = data.split(",");
                            String hphm=fields[0];
                            String clpp=fields[1];
                            String clys=fields[2];
                            String tgsj=fields[3];
                            String kkbh=fields[4];
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                            Date tgsj_date = sdf.parse(tgsj);
                            String sql = "insert into t_scrc_result (JSBH,HPHM,KKBH,TGSJ,CREATE_TIME) values(?,?,?,?,?)";
                            pstmt = conn.prepareStatement(sql);
                            long jsbh=System.currentTimeMillis();
                            pstmt.setString(1,jsbh+"_streaming");
                            pstmt.setString(2,hphm);
                            pstmt.setString(3,kkbh);
                            pstmt.setTimestamp(4,new Timestamp(tgsj_date.getTime()));
                            pstmt.setTimestamp(5,new Timestamp(jsbh));
                            pstmt.executeUpdate();
                        }
                        JdbcUtils.free(pstmt, conn);
                    }
                });
            }
        });
        ssc.start();
        ssc.awaitTermination();
        ssc.close();
    }
}
