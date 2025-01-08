package sbp;

import sbp.config.KafkaConfig;
import sbp.service.ThreadListener;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) {
        Properties propertiesConsumer1 = KafkaConfig.getKafkaProperties("consumer-1");
        Properties propertiesConsumer2 = KafkaConfig.getKafkaProperties("consumer-2");

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        executorService.submit(new ThreadListener(propertiesConsumer1));
        executorService.submit(new ThreadListener(propertiesConsumer2));
    }
}