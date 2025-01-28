package sbp.config;

import lombok.extern.slf4j.Slf4j;
import sbp.constants.Constants;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

@Slf4j
public class TopicPropertiesLoader {

    public static Properties getTopicProperties() {
        Properties appProps = new Properties();
        try (FileInputStream input = new FileInputStream(Constants.PATH_TO_TOPIC_PROPS)) {
            appProps.load(input);
        } catch (IOException e) {
            log.error("Ошибка при загрузке свойств topic: {}", e.getMessage());
        }

        return appProps;
    }
}
