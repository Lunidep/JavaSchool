package sbp.school.kafka;

import org.junit.jupiter.api.Test;
import sbp.school.kafka.dto.OperationType;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.service.ProducerService;

import java.util.Date;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;

class MainTest {
    @Test
    void testSendShouldDontThrowException() {
        ProducerService producerService = new ProducerService();
        TransactionDto transaction = new TransactionDto()
                .setTransactionId("111")
                .setAmount(555L)
                .setOperationType(OperationType.DEPOSIT)
                .setDate(new Date());
        assertThatNoException().isThrownBy(() -> producerService.send(transaction));
    }
}