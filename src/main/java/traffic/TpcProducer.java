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

		Properties kafkaProps = new Properties(); 

		kafkaProps.put("bootstrap.servers", "hadoop01:9092");

		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);

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
