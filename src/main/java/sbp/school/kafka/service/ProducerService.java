package sbp.school.kafka.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sbp.school.kafka.config.KafkaConfig;

import java.util.Properties;

public class ProducerService {
    private final Properties kafkaProperties;

    public ProducerService() {
        this.kafkaProperties = KafkaConfig.getKafkaProperties();
    }

    public void send() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProperties);
        producer.send(new ProducerRecord<>("quickstart-events", "Hello World"));
        producer.flush();
    }
}
