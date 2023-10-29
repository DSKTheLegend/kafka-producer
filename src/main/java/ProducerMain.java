import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;

public class ProducerMain {
    private static final Logger log = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

//        String bootstrapServers = "127.0.0.1:9092";\


        // create Producer properties
        log.info("Reading Kafka Config file");
        Properties properties = new Properties();
        String kafkaTopic;
        try {
            properties.load(ProducerMain.class.getResourceAsStream("kafka.conf"));

            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("bootstrapServers"));

            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            kafkaTopic = properties.getProperty("kafkaTopic");
        } catch (IOException e) {
            log.error(String.valueOf(e));
            throw new RuntimeException(e);
        }


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        log.info("Created Producer");
        // create a producer record
        Scanner scanner = new Scanner(System.in);
        ProducerRecord<String, String> producerRecord;
        log.info("Kafka Publisher Started");
        while (true) {
            System.out.print("\n[Publisher] >");
            String input = scanner.next();
            if (Objects.equals(input, "exit")) {
                break;
            }
            producerRecord = new ProducerRecord<>(kafkaTopic, input);

            // send data - asynchronous
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            });
            // flush data - synchronous
            producer.flush();
        }
        // flush and close producer
        producer.close();
    }

}
