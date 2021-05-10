package finalProject;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import kafka.StreamingKafkaProducer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONArray;

import scala.Tuple2;

public class Main {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static List<Record> records = new ArrayList<>();
	private static StreamingKafkaProducer streamingProducer;

	public static void main(String[] args) throws Exception
	{
		
		streamingProducer = new StreamingKafkaProducer();
		SparkConf sparkConf = new SparkConf().setAppName("projectSparkStreaming").setMaster("local");
		
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(13));
	
		String url = "https://data.seattle.gov/resource/fire-911.json?$$app_token=83GHgAaXwrGXR2mx8it9JiGDw&$where=datetime>='2021-05-09'";
		JavaDStream<String> dstream = ssc.receiverStream(new JavaCustomReceiver(url.trim()));
		
		JavaDStream<String> jsonStr = dstream.reduce((x,y) -> x + y);				
	
		String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		
		JavaPairDStream<String, String> pair = jsonStr.flatMapToPair(r -> {
			List<Tuple2<String, String>> pairs = new LinkedList<>();	
			
			if(r.toString().substring(r.toString().length()-1).equals("]") && r.toString().substring(0,1).equals("[")){
				JSONArray js1 = new JSONArray(r);
				for (int i = 0; i < js1.length(); i++) {
					String typeStr = js1.getJSONObject(i).get("type").toString();			
					String dateStr = js1.getJSONObject(i).get("datetime").toString();
					pairs.add(new Tuple2<String, String>(dateStr.substring(0,10),typeStr));
					
					Record record = new Record();
					record.setRecorddt(dateStr);
					record.setRecordtype(typeStr);	
					ObjectMapper Obj = new ObjectMapper();
					String jsonStr2 = Obj.writeValueAsString(record);
					streamingProducer.publishMsg(jsonStr2);
					records.add(record);	
				}
			}
			return pairs.iterator();
		});
				
		pair.dstream().saveAsTextFiles(args[1]+"/", "project");		
				
		ssc.start();
	
	    ssc.awaitTerminationOrTimeout(12000);
	    Thread.sleep(3000);
	    ssc.stop(true, true);
		
	    /* 
		if(records.size()>0){
		 SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark Hive Example")
				  .config("spark.sql.warehouse.dir", warehouseLocation)
				  .config("hive.metastore.warehouse.dir",warehouseLocation)
				  .enableHiveSupport()
				  .master("local[*]")
				  //.config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
				  //.config("spark.deploy.maxExecutorRetries", "10")
				  .getOrCreate();
			    
		    
			    Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
			    //recordsDF.createOrReplaceTempView("records");
			  	recordsDF.write().mode(SaveMode.Append).insertInto("records");
			  	
		} 
		*/
		
	  	//ssc.awaitTermination();
	 
	  	//recordsDF.write().mode(SaveMode.Append).saveAsTable("records");
		/*}
		catch(Exception e){
			System.out.println(e.getMessage());
		}*/
	    
	    
	}
}
class JavaSparkSessionSingleton {
	
	  private static transient SparkSession instance = null;
	  public static SparkSession getInstance(SparkConf sparkConf) {
		  String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
		  
		  //System.out.println("warehouseLocation:::::::::::::::::::" + warehouseLocation);
		  
	    if (instance == null) {
	      instance = SparkSession
	        .builder()
	         //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
		  //.config("hive.metastore.warehouse.dir","/user/hive/warehouse")
	        .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
	        .config(sparkConf)
	        .enableHiveSupport()
			.master("local[*]")
	        .getOrCreate();
	    }
	    return instance;
	  }
	}