package sbp.config;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class TransactionPropertiesLoader {

    public static Properties getTopicProperties() {
        Properties appProps = new Properties();
        try (InputStream input = KafkaProducerPropertiesLoader.class.getClassLoader().getResourceAsStream("transaction.properties")) {
            appProps.load(input);
        } catch (IOException e) {
            log.error("Ошибка при загрузке свойств topic: {}", e.getMessage());
        }

        return appProps;
    }
}
