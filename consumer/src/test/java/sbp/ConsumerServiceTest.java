package sbp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.config.KafkaConsumerPropertiesLoader;
import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.service.ConsumerService;
import sbp.storage.ackproducerstorage.TransactionStorage;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

@ExtendWith(MockitoExtension.class)
class ConsumerServiceTest {

    @Mock
    private TransactionStorage storage;

//    @InjectMocks
    private ConsumerService consumerService;

    private MockConsumer<String, TransactionDto> mockConsumer;
    private String topicName = "test-topic";

    @BeforeEach
    void setUp() {
        // Инициализация MockConsumer
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockConsumer.schedulePollTask(() -> mockConsumer.wakeup());

        Properties consumerProperties = KafkaConsumerPropertiesLoader.getKafkaConsumerProperties("test-group");
        consumerService = new ConsumerService(consumerProperties, storage);
        consumerService.setKafkaConsumer(mockConsumer);

        // Настройка топика и партиций
        TopicPartition partition = new TopicPartition(topicName, 0);
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(partition, 0L));
        mockConsumer.assign(Collections.singletonList(partition));
    }

    @Test
    void testConsumeValidMessage() {
        // Arrange
        TransactionDto transaction = new TransactionDto();
        transaction.setTransactionId("123");
        transaction.setOperationType(OperationType.DEPOSIT);
        transaction.setAmount(100L);
        transaction.setDate(LocalDateTime.now());

        ConsumerRecord<String, TransactionDto> record = new ConsumerRecord<>(
                topicName, 0, 0, "DEPOSIT", transaction
        );

        // Добавляем запись в мок-консьюмер
        mockConsumer.addRecord(record);

        // Act
        consumerService.consume();
    }
}