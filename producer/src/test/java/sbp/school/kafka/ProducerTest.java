package sbp.school.kafka;

import org.junit.jupiter.api.Test;
import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.config.KafkaProducerPropertiesLoader;
import sbp.school.kafka.service.ProducerService;

import java.util.Date;
import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

class ProducerTest {
    @Test
    void testSendShouldDontThrowException() {
        Properties producerProperties = KafkaProducerPropertiesLoader.getKafkaProducerProperties();
        ProducerService producerService = new ProducerService(producerProperties);
        TransactionDto transaction = new TransactionDto()
                .setTransactionId("111")
                .setAmount(555L)
                .setOperationType(OperationType.DEPOSIT)
                .setDate(new Date());
        assertThatNoException().isThrownBy(() -> producerService.send(transaction));
    }
}