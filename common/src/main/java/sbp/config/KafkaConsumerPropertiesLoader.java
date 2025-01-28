package sbp.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import sbp.constants.Constants;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class KafkaConsumerPropertiesLoader {

    public static Properties getKafkaConsumerProperties(String groupId) {
        Properties appProps = new Properties();
        try (InputStream input = KafkaConsumerPropertiesLoader.class.getClassLoader().getResourceAsStream("kafka-consumer.properties")) {
            appProps.load(input);
        } catch (IOException e) {
            log.error("Ошибка при загрузке свойств consumer: {}", e.getMessage());
        }

        appProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return appProps;
    }
}
