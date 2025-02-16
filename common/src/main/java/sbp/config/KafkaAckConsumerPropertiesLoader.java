package sbp.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class KafkaAckConsumerPropertiesLoader {

    public static Properties getKafkaAckConsumerProperties(String groupId) {
        Properties appProps = new Properties();
        try (InputStream input = KafkaAckConsumerPropertiesLoader.class.getClassLoader().getResourceAsStream("kafka-ack-consumer.properties")) {
            appProps.load(input);
        } catch (IOException e) {
            log.error("Ошибка при загрузке свойств consumer: {}", e.getMessage());
        }

        appProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return appProps;
    }
}
