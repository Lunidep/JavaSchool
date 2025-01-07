package sbp.config;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class KafkaAckProducerPropertiesLoader {

    public static Properties getKafkaAckProducerProperties() {
        Properties appProps = new Properties();
        try (InputStream input = KafkaProducerPropertiesLoader.class.getClassLoader().getResourceAsStream("kafka-ack-producer.properties")) {
            appProps.load(input);
        } catch (IOException e) {
            log.error("Ошибка при загрузке свойств Kafka: {}", e.getMessage());
        }

        return appProps;
    }
}
