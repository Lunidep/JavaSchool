package sbp;

import org.junit.jupiter.api.Test;
import sbp.config.KafkaConsumerPropertiesLoader;
import sbp.service.ThreadListener;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNull;

public class ConsumerServiceTest {

    @Test
    void testGetMessageShouldDontThrowException() throws InterruptedException {
        Properties propertiesConsumer1 = KafkaConsumerPropertiesLoader.getKafkaConsumerProperties("consumer-1");
        Properties propertiesConsumer2 = KafkaConsumerPropertiesLoader.getKafkaConsumerProperties("consumer-2");

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        AtomicReference<Exception> exceptionRef = new AtomicReference<>(null);

        executorService.submit(() -> {
            try {
                new ThreadListener(propertiesConsumer1).start();
            } catch (Exception e) {
                exceptionRef.set(e);
            }
        });

        executorService.submit(() -> {
            try {
                new ThreadListener(propertiesConsumer2).start();
            } catch (Exception e) {
                exceptionRef.set(e);
            }
        });

        TimeUnit.SECONDS.sleep(10);
        executorService.shutdown();

        assertNull(exceptionRef.get(), "Исключение было выброшено: " + exceptionRef.get());
    }
}