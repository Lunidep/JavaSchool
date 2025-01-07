package sbp.school.kafka.config;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

@Slf4j
public class KafkaProducerPropertiesLoader {

    public static Properties getKafkaProducerProperties() {
        Properties appProps = new Properties();
        try (FileInputStream input = new FileInputStream("src/main/resources/kafka-producer.properties")) {
            appProps.load(input);
        } catch (IOException e) {
            log.error("Ошибка при загрузке свойств Kafka: {}", e.getMessage());
        }

        return appProps;
    }
}
