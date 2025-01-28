package sbp.school.kafka;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.config.KafkaProducerPropertiesLoader;
import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.school.kafka.service.ProducerService;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Slf4j
public class AckProducerTest {
    private Properties producerProperties;
    private ProducerService producerService;

    @BeforeEach
    void setUp() {
        producerProperties = KafkaProducerPropertiesLoader.getKafkaProducerProperties();
        producerService = new ProducerService(producerProperties);
    }

    @Test
    void testSendMultipleTransactions_NoException() {
        List<TransactionDto> transactions = createTestTransactions();

        assertDoesNotThrow(() -> transactions.forEach(transaction -> {
            producerService.send(transaction);
            log.info("Тестовая транзакция отправлена: {}", transaction);
        }));
    }

    private static List<TransactionDto> createTestTransactions() {
        return Arrays.asList(
                new TransactionDto()
                        .setTransactionId("111")
                        .setAmount(111L)
                        .setOperationType(OperationType.DEPOSIT)
                        .setDate(new Date()),
                new TransactionDto()
                        .setTransactionId("222")
                        .setAmount(222L)
                        .setOperationType(OperationType.DEPOSIT)
                        .setDate(new Date()),
                new TransactionDto()
                        .setTransactionId("333")
                        .setAmount(333L)
                        .setOperationType(OperationType.DEPOSIT)
                        .setDate(new Date()),
                new TransactionDto()
                        .setTransactionId("444")
                        .setAmount(444L)
                        .setOperationType(OperationType.WITHDRAWAL)
                        .setDate(new Date()),
                new TransactionDto()
                        .setTransactionId("555")
                        .setAmount(555L)
                        .setOperationType(OperationType.WITHDRAWAL)
                        .setDate(new Date()),
                new TransactionDto()
                        .setTransactionId("666")
                        .setAmount(666L)
                        .setOperationType(OperationType.WITHDRAWAL)
                        .setDate(new Date())
        );
    }
}
