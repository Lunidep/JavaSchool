package sbp.school.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import sbp.config.KafkaProducerPropertiesLoader;
import sbp.config.TransactionPropertiesLoader;
import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.school.kafka.service.ProducerService;
import sbp.storage.InProcessTransactionsStorage;
import sbp.storage.SentTransactionsStorage;
import sbp.storage.Storage;
import sbp.storage.impl.InProcessTransactionsStorageImpl;
import sbp.storage.impl.SentTransactionsStorageImpl;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AckProducerTest {
    private ProducerService producerService;
    private final SentTransactionsStorage sentTransactionsStorage;
    private final InProcessTransactionsStorage inProcessTransactionsStorage;

    private final Storage storage;
    private final Properties transactionsConfig;

    public AckProducerTest() {
        storage = new Storage();
        inProcessTransactionsStorage = new InProcessTransactionsStorageImpl(storage);
        sentTransactionsStorage = new SentTransactionsStorageImpl(storage);

        transactionsConfig = TransactionPropertiesLoader.getTopicProperties();
    }

    @BeforeEach
    void setUp() {
        Properties producerProperties = KafkaProducerPropertiesLoader.getKafkaProducerProperties();
        producerService = new ProducerService(producerProperties, storage);
    }

    @Test
    @Order(1)
    void testSendMultipleTransactions_NoException() {
        List<TransactionDto> transactions = createTestTransactions();

        assertDoesNotThrow(() -> transactions.forEach(transaction -> {
            producerService.send(transaction);
            log.info("Тестовая транзакция отправлена: {}", transaction);
        }));
    }

    @Test
    @Order(2)
    void testSendTransactionsAndWaitForAcknowledgements() throws InterruptedException {
        ScheduledExecutorService scheduler = startTransactionRetryScheduler();

        // Даем время на настройку Kafka и других компонентов
        Thread.sleep(1000);

        sendTestTransactions();
        log.info("Ждем готовности...");
        Thread.sleep(5000);

        waitForAllAcks();

        log.info("Для всех отправленных транзакций получены подтверждения, либо превышено количество переотправок!");
    }

    private ScheduledExecutorService startTransactionRetryScheduler() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Duration retryPeriod = Duration.parse(transactionsConfig.getProperty("transaction.retry.period"));
        Runnable retryTask = () -> {
            try {
                producerService.run();
            } catch (Exception e) {
                throw new RuntimeException("Ошибка при повторной отправке транзакций", e);
            }
        };
        scheduler.scheduleAtFixedRate(retryTask, 0, retryPeriod.toMillis(), TimeUnit.MILLISECONDS);
        return scheduler;
    }

    private void sendTestTransactions() {
        List<TransactionDto> transactionsToSend = createTestTransactions();
        transactionsToSend.forEach(transaction -> {
            log.info("Начинаем отправку тестовой транзакции: {}", transaction);
            producerService.send(transaction);
            log.info("Тестовая транзакция отправлена: {}", transaction);
        });
    }

    private void waitForAllAcks() throws InterruptedException {
        while (!sentTransactionsStorage.isSentTransactionsEmpty() || !inProcessTransactionsStorage.isTransactionsSendInProgressEmpty()) {
            Thread.sleep(100);
        }
    }

    private static List<TransactionDto> createTestTransactions() {
        return Arrays.asList(
                new TransactionDto()
                        .setTransactionId("111")
                        .setAmount(111L)
                        .setOperationType(OperationType.DEPOSIT)
                        .setDate(LocalDateTime.now()    ),
                new TransactionDto()
                        .setTransactionId("222")
                        .setAmount(222L)
                        .setOperationType(OperationType.DEPOSIT)
                        .setDate(LocalDateTime.now()),
                new TransactionDto()
                        .setTransactionId("333")
                        .setAmount(333L)
                        .setOperationType(OperationType.DEPOSIT)
                        .setDate(LocalDateTime.now()),
                new TransactionDto()
                        .setTransactionId("444")
                        .setAmount(444L)
                        .setOperationType(OperationType.WITHDRAWAL)
                        .setDate(LocalDateTime.now()),
                new TransactionDto()
                        .setTransactionId("555")
                        .setAmount(555L)
                        .setOperationType(OperationType.WITHDRAWAL)
                        .setDate(LocalDateTime.now()),
                new TransactionDto()
                        .setTransactionId("666")
                        .setAmount(666L)
                        .setOperationType(OperationType.WITHDRAWAL)
                        .setDate(LocalDateTime.now())
        );
    }
}
