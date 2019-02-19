package traffic;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
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
 * 套牌车布控算法实现
 * author:brave
 */
public class TpcStreamingCompute {
    public static void main(String[] args)throws InterruptedException{
        SparkSession spark=SparkSession.builder().master("local[2]").appName("ScrcStreamingCompute").getOrCreate();
        //获取疑似套牌车数据t_tpc_result
        Dataset<Row> tpcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://bigdata03:3306/test?characterEncoding=UTF-8")
                .option("dbtable", "t_tpc_result")
                .option("user", "root")
                .option("password", "123456")
                .load();
        Dataset<Row> tpcHphmDF = tpcDF.select("hphm").distinct();
        tpcHphmDF.cache();//提高性能
        tpcHphmDF.show();
        JavaRDD<Row> tpcRDD = tpcHphmDF.javaRDD();
        JavaPairRDD<String, String> tpcPairRDD = tpcRDD.mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String hphm = row.getAs("hphm");
                return new Tuple2<String, String>(hphm, hphm);
            }
        });
        spark.sparkContext().setLogLevel("ERROR");
        //接着使用spark streaming获取kafka中的数据
        JavaSparkContext jsc=new JavaSparkContext(spark.sparkContext());
        //kafka broker地址，多个地址用逗号分隔
        String brokers = "bigdata01:9092";
        //kafka topic名称,多个名称用逗号分隔
        String topics = "cnwTopic";
        //创建流处理上下文对象;JavaStreamingContext
        JavaStreamingContext ssc = new JavaStreamingContext(jsc, Durations.seconds(6));
        //创建topic集合变量，用于KafkaUtils.createDirectStream方法
        Collection<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        //创建kafka相关参数变量，用于KafkaUtils.createDirectStream方法
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "tpc_group");
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //创建Topic分区对象，用于KafkaUtils.createDirectStream方法
        Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();
        //jdbc获取offset信息
//        offsets.put(new TopicPartition("topic1", 0), 2L);
        /**
         * KafkaUtils老版本的用法，参见spark软件包中的example目录中的相关例子。
         * 本项目采用新版本的用法 (http://spark.apache.org/docs/2.1.3/streaming-kafka-0-10-integration.html)。
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
        JavaInputDStream<ConsumerRecord<String,String>> kafkaDStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams, offsets)
        );
        //注意:kafkaDStream里是ConsumerRecord对象
        JavaPairDStream<String, String> pairDStream = kafkaDStream.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<String, String> message) throws Exception {
                List<String> result = new ArrayList<String>();
                String messageValue = message.value();//json数组
                //使用GSON工具解析json数组
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
            }//返回的数据格式：(HPHM_CLPP,CLYS,CLYS,TGSJ,KKBH)
        }).mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split("_")[0], s.split("_")[1]);
            }
        });
//        pairDStream.print();
        //DStream的transform方法，遍历流中的RDD，转换为RDD级别的数据计算(Spark Streaming --> Spark Core)
        JavaDStream<String> resultDStream = pairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> pairRDD) throws Exception {
                //将流中的每一个RDD与tpcPairRDD进行join操作(内连接)，然后使用map算子遍历join结果，返回JavaRDD<String>对象
                JavaRDD<String> resultRDD = pairRDD.join(tpcPairRDD).map(new Function<Tuple2<String, Tuple2<String, String>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, String>> t2) throws Exception {
                        return t2._1+","+t2._2._1;
                    }
                });
                return resultRDD;
            }
        });
//        resultDStream.print();
        //DStream数据输出到外部存储，常用方式。重要!!!!!!
        resultDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> iter) throws Exception {
                        Connection conn = JdbcUtils.getConnection();
                        conn.setAutoCommit(false);


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
                            String sql = "insert into t_tpc_result (JSBH,HPHM,CLPP,CLYS,TGSJ,KKBH,CREATE_TIME) values(?,?,?,?,?,?,?)";
                            pstmt = conn.prepareStatement(sql);
                            long jsbh=System.currentTimeMillis();
                            pstmt.setString(1,jsbh+"_streaming");
                            pstmt.setString(2,hphm);
                            pstmt.setString(3,clpp);
                            pstmt.setString(4,clys);
                            pstmt.setTimestamp(5,new Timestamp(tgsj_date.getTime()));
                            pstmt.setString(6,kkbh);
                            pstmt.setTimestamp(7,new Timestamp(jsbh));
                            pstmt.executeUpdate();
                        }
                        conn.commit();
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
