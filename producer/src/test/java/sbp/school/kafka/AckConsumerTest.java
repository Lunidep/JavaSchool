package sbp.school.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.*;
import sbp.config.KafkaAckConsumerPropertiesLoader;
import sbp.config.KafkaAckProducerPropertiesLoader;
import sbp.config.KafkaProducerPropertiesLoader;
import sbp.config.TransactionPropertiesLoader;
import sbp.dto.AckDto;
import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.school.kafka.ack.consumer.AckConsumerService;
import sbp.school.kafka.service.ProducerService;
import sbp.storage.ackconsumerstorage.InProcessTransactionsStorage;
import sbp.storage.ackconsumerstorage.SentTransactionsStorage;
import sbp.storage.ackconsumerstorage.Storage;
import sbp.storage.ackconsumerstorage.impl.InProcessTransactionsStorageImpl;
import sbp.storage.ackconsumerstorage.impl.SentTransactionsStorageImpl;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AckConsumerTest {
    private ProducerService producer;
    private final SentTransactionsStorage sentTransactionsStorage;
    private final InProcessTransactionsStorage inProcessTransactionsStorage;

    private final Storage storage;
    private final Properties transactionsConfig;
    private final Properties ackConsumerConfig;

    public AckConsumerTest() {
        storage = new Storage();
        inProcessTransactionsStorage = new InProcessTransactionsStorageImpl(storage);
        sentTransactionsStorage = new SentTransactionsStorageImpl(storage);

        transactionsConfig = TransactionPropertiesLoader.getTopicProperties();
        ackConsumerConfig = KafkaAckConsumerPropertiesLoader.getKafkaAckConsumerProperties("group-1");

    }

    @BeforeEach
    void setUp() {
        Properties producerProperties = KafkaProducerPropertiesLoader.getKafkaProducerProperties();
        producer = new ProducerService(producerProperties, storage);
    }

    @Test
    @Order(1)
    void testSendMultipleTransactions_NoException() {
        List<TransactionDto> transactions = createTestTransactions();

        assertDoesNotThrow(() -> transactions.forEach(transaction -> {
            producer.send(transaction);
            log.info("Тестовая транзакция отправлена: {}", transaction);
        }));
    }

    @Test
    @Order(2)
    void testSendTransactionsAndWaitForAcknowledgements() {
        ScheduledExecutorService scheduler = null;
        try (AckConsumerService ackConsumer = new AckConsumerService(ackConsumerConfig, storage)) {
            startAckConsumer(ackConsumer);
            scheduler = startTransactionRetryScheduler();

            // Даем время на настройку Kafka и других компонентов
            Thread.sleep(1000);

            sendTestTransactions();
            log.info("Ждем готовности...");
            Thread.sleep(5000);

            waitForAllAcks();

            log.info("Для всех отправленных транзакций получены подтверждения, либо превышено количество переотправок!");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Тест был прерван", e);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка во время выполнения теста", e);
        } finally {
            shutdownScheduler(scheduler);
        }
    }

    @Test
    @Order(3)
    void testSendTransactionsAndWaitForSuccessAcknowledgements() {
        ScheduledExecutorService scheduler = null;
        try (AckConsumerService ackConsumer = new AckConsumerService(ackConsumerConfig, storage)) {
            startAckConsumer(ackConsumer);
            scheduler = startTransactionRetryScheduler();

            // Даем время на настройку Kafka и других компонентов
            Thread.sleep(1000);

            sendTestTransactions();
            log.info("Ждем готовности...");
            Thread.sleep(5000);

            sendTestAcks(storage.getSentChecksumMap());

            waitForAllAcks();

            log.info("Для всех отправленных транзакций получены подтверждения, либо превышено количество переотправок!");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Тест был прерван", e);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка во время выполнения теста", e);
        } finally {
            shutdownScheduler(scheduler);
        }
    }

    private void startAckConsumer(AckConsumerService ackConsumer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                ackConsumer.close();
            } catch (Exception e) {
                throw new RuntimeException("Ошибка при закрытии AckConsumer", e);
            }
        }));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(ackConsumer);
    }

    private ScheduledExecutorService startTransactionRetryScheduler() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Duration retryPeriod = Duration.parse(transactionsConfig.getProperty("transaction.retry.period"));
        Runnable retryTask = () -> {
            try {
                producer.run();
            } catch (Exception e) {
                throw new RuntimeException("Ошибка при повторной отправке транзакций", e);
            }
        };
        scheduler.scheduleAtFixedRate(retryTask, 0, retryPeriod.toMillis(), TimeUnit.MILLISECONDS);
        return scheduler;
    }

    private void sendTestTransactions() throws InterruptedException {
        List<TransactionDto> transactionsToSend = createTestTransactions();
        transactionsToSend.forEach(transaction -> {
            log.info("Начинаем отправку тестовой транзакции: {}", transaction);
            producer.send(transaction);
            log.info("Тестовая транзакция отправлена: {}", transaction);
        });
    }

    private void waitForAllAcks() throws InterruptedException {
        while (!sentTransactionsStorage.isSentTransactionsEmpty() || !inProcessTransactionsStorage.isTransactionsSendInProgressEmpty()) {
            Thread.sleep(100);
        }
    }

    private void shutdownScheduler(ScheduledExecutorService scheduler) {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

    private void sendTestAcks(Map<Long, String> sentChecksums) {
        KafkaProducer<String, AckDto> ackProducer = createAckProducer();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        Runnable producerTask = () -> sendAckMessage(ackProducer, sentChecksums);

        scheduler.scheduleAtFixedRate(producerTask, 0, 6, TimeUnit.SECONDS);
    }

    private KafkaProducer<String, AckDto> createAckProducer() {
        Properties ackProducerProperties = KafkaAckProducerPropertiesLoader.getKafkaAckProducerProperties();
        return new KafkaProducer<>(ackProducerProperties);
    }

    private void sendAckMessage(KafkaProducer<String, AckDto> ackProducer, Map<Long, String> sentChecksums) {
        Map.Entry<Long, String> checksumEntry = sentChecksums.entrySet().iterator().next();
        AckDto ackDto = getAckDto(checksumEntry);

        ProducerRecord<String, AckDto> record = createProducerRecord(ackDto);

        sendRecordToKafka(ackProducer, record);
    }

    private ProducerRecord<String, AckDto> createProducerRecord(AckDto ackDto) {
        String topicName = transactionsConfig.getProperty("transaction.ack.topic.name");
        return new ProducerRecord<>(topicName, ackDto.getIntervalKey(), ackDto);
    }

    private void sendRecordToKafka(KafkaProducer<String, AckDto> ackProducer, ProducerRecord<String, AckDto> record) {
        ackProducer.send(record, (metadata, exception) -> {
            if (exception == null) {
                log.info("Тестовое подтверждение {} отправлено в топик {}, offset={}", record.value(), record.topic(), metadata.offset());
            } else {
                log.error("Ошибка отправки тестового подтверждения", exception);
            }
        });
    }

    private AckDto getAckDto(Map.Entry<Long, String> entry) {
        Long key = entry.getKey();
        String value = getCheckSumWithHalfProbability(entry);

        return new AckDto(key.toString(), value);
    }

    private String getCheckSumWithHalfProbability(Map.Entry<Long, String> entry) {
        Random random = new Random();
        // Вероятность 50%
        if (random.nextBoolean()) {
            return entry.getValue(); // Верная контрольная сумма
        } else {
            return "f".repeat(32); // Неверная контрольная сумма
        }
    }

    private static List<TransactionDto> createTestTransactions() {
        return Arrays.asList(
                new TransactionDto()
                        .setTransactionId("111")
                        .setAmount(111L)
                        .setOperationType(OperationType.DEPOSIT)
                        .setDate(LocalDateTime.now()),
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
