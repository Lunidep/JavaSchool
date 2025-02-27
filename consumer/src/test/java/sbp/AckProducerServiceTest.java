package sbp;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import sbp.ack.producer.AckProducerService;
import sbp.config.KafkaAckProducerPropertiesLoader;
import sbp.config.KafkaConsumerPropertiesLoader;
import sbp.config.KafkaProducerPropertiesLoader;
import sbp.config.TransactionPropertiesLoader;
import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.school.kafka.service.ProducerService;
import sbp.service.ConsumerService;
import sbp.storage.ackconsumerstorage.Storage;
import sbp.storage.ackproducerstorage.TransactionStorage;
import sbp.storage.ackproducerstorage.impl.TransactionStorageImpl;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AckProducerServiceTest {
    private ConsumerService consumer;
    private ProducerService producer;

    private AckProducerService ackProducer;

    private TransactionStorage transactionAckConsumerStorage;

    private Duration ackTimeout;

    @BeforeEach
    void setUp() {
        transactionAckConsumerStorage = new TransactionStorageImpl();
        Storage transactionAckProducerStorage = new Storage();

        Properties transactionProperties = TransactionPropertiesLoader.getTopicProperties();
        this.ackTimeout = Duration.parse(transactionProperties.getProperty("transaction.ack.timeout"));

        Properties ackConsumerProperties = KafkaConsumerPropertiesLoader.getKafkaConsumerProperties("group1");
        consumer = new ConsumerService(ackConsumerProperties, transactionAckConsumerStorage);

        Properties ackProducerProperties = KafkaAckProducerPropertiesLoader.getKafkaAckProducerProperties();
        ackProducer = new AckProducerService(ackProducerProperties, transactionAckConsumerStorage);

        Properties producerProperties = KafkaProducerPropertiesLoader.getKafkaProducerProperties();
        producer = new ProducerService(producerProperties, transactionAckProducerStorage);
    }

    @AfterEach
    void close() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void transactionsConsumeTest_And_SendAcks() throws InterruptedException {
        ScheduledExecutorService scheduler = null;
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

            ExecutorService executor = Executors.newSingleThreadExecutor();
            scheduler = Executors.newScheduledThreadPool(1);
            Runnable ackTask = ackProducer;

            executor.submit(consumer);
            scheduler.scheduleAtFixedRate(ackTask, 0, ackTimeout.toMillis(), TimeUnit.MILLISECONDS);

            sendTestTransactions();

            log.info("Ждем готовности...");
            Thread.sleep(5000);

            while (!transactionAckConsumerStorage.isEmpty()) {
                Thread.sleep(100);
            }
            log.info("Для всех полученных транзакций подтверждения отправлены");
        } finally {
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }

    private void sendTestTransactions() {
        List<TransactionDto> testTransactions = createTestTransactions();
        for (TransactionDto transaction : testTransactions) {
            producer.send(transaction);
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