package sbp.school.kafka;

import org.junit.jupiter.api.Test;
import sbp.config.KafkaProducerPropertiesLoader;
import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.school.kafka.service.ProducerService;
import sbp.storage.Storage;

import java.time.LocalDateTime;
import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

class ProducerTest {
    @Test
    void testSendShouldDoNotThrowException() {
        Storage storage = new Storage();
        Properties producerProperties = KafkaProducerPropertiesLoader.getKafkaProducerProperties();
        ProducerService producerService = new ProducerService(producerProperties, storage);
        TransactionDto transaction = new TransactionDto()
                .setTransactionId("111")
                .setAmount(555L)
                .setOperationType(OperationType.DEPOSIT)
                .setDate(LocalDateTime.now());
        assertThatNoException().isThrownBy(() -> producerService.send(transaction));
    }
}