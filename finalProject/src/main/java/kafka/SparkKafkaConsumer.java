package kafka;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;

import scala.Tuple2;
import kafka.serializer.StringDecoder;

public class SparkKafkaConsumer {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		System.out.println("Spark Streaming started now .....");

		SparkConf conf = new SparkConf().setAppName("Final Project").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("mainTopic");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		  List<String> allRecord = new ArrayList<String>();
		  final String COMMA = ",";
		  
		
//		JavaDStream<String> jsonStr = directKafkaStream.reduce((x,y) -> x + y);
//		JavaPairDStream<String, String> pair = jsonStr.flatMapToPair(r -> {
//		List<Tuple2<String, String>> pairs = new LinkedList<>();	
//			
//			if(r.toString().substring(r.toString().length()-1).equals("]") && r.toString().substring(0,1).equals("[")){
//				JSONArray js1 = new JSONArray(r);
//				for (int i = 0; i < js1.length(); i++) {
//					String typeStr = js1.getJSONObject(i).get("type").toString();			
//					String dateStr = js1.getJSONObject(i).get("datetime").toString();
//					pairs.add(new Tuple2<String, String>(dateStr.substring(0,10),typeStr));
//					
//					Record record = new Record();
//					record.setRecorddt(dateStr);
//					record.setRecordtype(typeStr);	
//					ObjectMapper Obj = new ObjectMapper();
//					String jsonStr2 = Obj.writeValueAsString(record);
//					
//					records.add(record);	
//				}
//			}
//			return pairs.iterator();
//		});
				
		  directKafkaStream.dstream().saveAsTextFiles("output/", "project");		
		  	
//		ssc.start();
//	
//	    ssc.awaitTerminationOrTimeout(12000);
//	    Thread.sleep(3000);
//	    ssc.stop(true, true);
		   
		    
		ssc.start();
		ssc.awaitTermination();
	}
	

}
