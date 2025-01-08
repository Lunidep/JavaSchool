package sbp.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerService {
    public void read(Properties properties) {
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of("transactions-topic"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    log.info(String.format("consumer: %s, topic: %s, partition: %d, key: %s, value: %s, offset: %d",
                            properties.get("group.id"), record.topic(), record.partition(), record.key(), record.value(), record.offset()));
                }
            }
        }
    }
}

