package sbp.school.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import sbp.config.KafkaProducerPropertiesLoader;
import sbp.config.TransactionPropertiesLoader;
import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.school.kafka.service.ProducerService;
import sbp.school.kafka.utils.transaction.TransactionPartitioner;
import sbp.storage.ackconsumerstorage.Storage;
import sbp.utils.serializers.TransactionSerializer;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProducerTest {

    private MockProducer<String, TransactionDto> mockProducer;
    private ProducerService producerService;
    private String topicName;
    @Mock
    private TransactionSerializer transactionSerializer;


    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    static {
        OBJECT_MAPPER.registerModule(new JavaTimeModule());
        OBJECT_MAPPER.setDateFormat(new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS));
    }

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(
                Cluster.empty(),
                true,
                new TransactionPartitioner(),
                new StringSerializer(),
                transactionSerializer
        );
        Properties producerProperties = KafkaProducerPropertiesLoader.getKafkaProducerProperties();
        Storage storage = new Storage();
        producerService = new ProducerService(producerProperties, storage);
        producerService.setKafkaProducer(mockProducer);

        Properties transactionProperties = TransactionPropertiesLoader.getTopicProperties();
        this.topicName = transactionProperties.getProperty("transaction.topic.name");


    }

    @Test
    void testSendTransaction() throws JsonProcessingException {
        TransactionDto transaction = new TransactionDto();
        transaction.setTransactionId("123");
        transaction.setOperationType(OperationType.DEPOSIT);
        transaction.setAmount(100L);
        transaction.setDate(LocalDateTime.now());

        when(transactionSerializer.serialize(topicName, transaction))
                .thenReturn(OBJECT_MAPPER.writeValueAsBytes(transaction));

        producerService.send(transaction);

        List<ProducerRecord<String, TransactionDto>> records = mockProducer.history();
        assertEquals(1, records.size());

        ProducerRecord<String, TransactionDto> sentRecord = records.get(0);
        assertEquals(topicName, sentRecord.topic());
        assertEquals(OperationType.DEPOSIT.name(), sentRecord.key());
        assertEquals(transaction, sentRecord.value());
    }
}