package kafka;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


public class StreamingKafkaProducer {

	private static String KafkaBrokerEndpoint = "localhost:9092";
    private static String KafkaTopic = "mainTopic";
    private static String CsvFile = "test.csv";
    
    private Producer<String, String> ProducerProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) throws URISyntaxException {
    	StreamingKafkaProducer kafkaProducer = new StreamingKafkaProducer();
        kafkaProducer.PublishMessages();
        System.out.println("Producing job completed");
    }
    
    public void publishMsg(String line) {
    	
    	final Producer<String, String> jsonProducer = ProducerProperties();
    	final ProducerRecord<String, String> jsonRecord = new ProducerRecord<String, String>(
                KafkaTopic, UUID.randomUUID().toString(), line);

    	jsonProducer.send(jsonRecord, (metadata, exception) -> {
            if(metadata != null){
                System.out.println("JSONData: -> "+ jsonRecord.key()+" | "+ jsonRecord.value());
            }
            else{
                System.out.println("Error Sending Csv Record -> "+ jsonRecord.value());
            }
        });
    }

    private void PublishMessages() throws URISyntaxException{
        final Producer<String, String> csvProducer = ProducerProperties();
        
        try{
        	URI uri = getClass().getClassLoader().getResource(CsvFile).toURI(); 
            Stream<String> FileStream = Files.lines(Paths.get(uri));
            
            FileStream.forEach(line -> {
            	System.out.println(line);
                final ProducerRecord<String, String> jsonRecord = new ProducerRecord<String, String>(
                        KafkaTopic, UUID.randomUUID().toString(), line);

                csvProducer.send(jsonRecord, (metadata, exception) -> {
                    if(metadata != null){
                        System.out.println("JSONData: -> "+ jsonRecord.key()+" | "+ jsonRecord.value());
                    }
                    else{
                        System.out.println("Error Sending JSON Record -> "+ jsonRecord.value());
                    }
                });
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    

    
    
    
}