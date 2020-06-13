package cs523.kafka;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

public class SampleProducer {

	private static String KAFKA_BROKER_ENDPOINT = null;
	private static String KAFKA_TOPIC = null;
	private static String CSV_FILE = null;

	public static void main(String[] args) throws URISyntaxException {
		//if (args != null) {
			KAFKA_BROKER_ENDPOINT =  "localhost:9092";
			KAFKA_TOPIC = "mytesttopic";
			CSV_FILE = "test.csv";
		//}
		System.out.println("Hello ");
		SampleProducer kafkaCsvProducer = new SampleProducer();
		kafkaCsvProducer.publishMessages();
	}

	private Producer<String, String> ProducerProperties() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				KAFKA_BROKER_ENDPOINT);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "SampleProducer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
//        properties.put(ProducerConfig.ACKS_CONFIG,
//                "all");
//        properties.put(ProducerConfig.LINGER_MS_CONFIG,
//                1);
//        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,
//                163844);
//        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
//                335544322);

		return new KafkaProducer<String, String>(properties);
	}

	private void publishMessages() {

		final Producer<String, String> csvProducer = ProducerProperties();
		try {
			Stream<String> fileStream = Files.lines(Paths.get(CSV_FILE));

//			URI uri = getClass().getClassLoader().getResource("/home/cloudera/workspace/kafka/test.csv").toURI();
//            Stream<String> fileStream = Files.lines(Paths.get(uri));
			fileStream
					.forEach(line -> {
						final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(
								KAFKA_TOPIC, UUID.randomUUID().toString(), line);
						csvProducer.send(csvRecord, (metadata, exception) -> {
							if (metadata != null) {
								System.out.println("CsvData: -> "
										+ csvRecord.key() + " | "
										+ csvRecord.value());
							} else {
								System.out
										.println("Error Sending CSV Record -> "
												+ csvRecord.value());
							}
						});
					});
			System.out.println("Done");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
