package sbp;

import org.junit.jupiter.api.Test;
import sbp.config.KafkaConsumerPropertiesLoader;
import sbp.service.ConsumerService;
import sbp.storage.ackproducerstorage.TransactionStorage;
import sbp.storage.ackproducerstorage.impl.TransactionStorageImpl;

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

        TransactionStorage transactionStorage1 = new TransactionStorageImpl();
        executorService.submit(() -> {
            try {
                new ConsumerService(propertiesConsumer1, transactionStorage1).start();
            } catch (Exception e) {
                exceptionRef.set(e);
            }
        });

        TransactionStorage transactionStorage2 = new TransactionStorageImpl();
        executorService.submit(() -> {
            try {
                new ConsumerService(propertiesConsumer2, transactionStorage2).start();
            } catch (Exception e) {
                exceptionRef.set(e);
            }
        });

        TimeUnit.SECONDS.sleep(10);
        executorService.shutdown();

        assertNull(exceptionRef.get(), "Исключение было выброшено: " + exceptionRef.get());
    }
}