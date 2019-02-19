package traffic;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

/**
 * 套牌车布控流处理所需kafka数据生产者
 */
public class TpcProducer {

	public static void main(String[] args) {
		//创建一个Properties对象，用于存储连接kafka所需要的配置信息
		Properties kafkaProps = new Properties(); 
		//配置kafka集群地址--如果此处使用主机名bigdata01，需要在当前电脑的hosts文件中配置映射
		kafkaProps.put("bootstrap.servers", "bigdata01:9092");
		//向kafka集群发送消息,除了消息值本身,还包括key信息,key信息用于消息在partition之间均匀分布。
		//发送消息的key,类型为String,使用String类型的序列化器
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//发送消息的value,类型为String,使用String类型的序列化器
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//创建一个KafkaProducer对象，传入上面创建的Properties对象
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
		/**
		 * 使用ProducerRecord<String, String>(String topic, String key, String value)构造函数创建消息对象。构造函数接受三个参数：
		 * topic--告诉kafkaProducer消息发送到哪个topic;
		 * key--告诉kafkaProducer，所发送消息的key值，注意：key值类型需与前面设置的key.serializer值匹配
		 * value--告诉kafkaProducer，所发送消息的value值，即消息内容。注意：value值类型需与前面设置的value.serializer值匹配
		 */
		Gson gson=new Gson();
		List<Map<String,String>> message=new ArrayList<Map<String,String>>();
		for(int i=0;i<1;i++){
			Map<String,String> data=new HashMap<String, String>();
			data.put("HPHM", "粤AL9786");
			data.put("CLPP", "大众");
			data.put("CLYS", "白");
			data.put("TGSJ", "2018-09-26 17:35:00");
			data.put("KKBH","798798594");
			message.add(data);
		}
		String jsonData = gson.toJson(message);
		System.out.println(jsonData);
		ProducerRecord<String, String> record =new ProducerRecord<String, String>("cnwTopic",  jsonData); 
		try {
		  //发送前面创建的消息对象ProducerRecord到kafka集群。发送消息过程中可能发送错误，如无法连接kafka集群，所以在这里使用捕获异常代码
		  producer.send(record); 
		  //关闭kafkaProducer对象
		  producer.close();
		} catch (Exception e) {
		    e.printStackTrace(); 
		}
	}
}
