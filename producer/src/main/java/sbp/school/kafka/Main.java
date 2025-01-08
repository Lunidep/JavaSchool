package sbp.school.kafka;

import sbp.school.kafka.dto.OperationType;
import sbp.school.kafka.dto.TransactionDto;
import sbp.school.kafka.service.ProducerService;

import java.util.Date;

public class Main {
    public static void main(String[] args) {
        ProducerService producerService = new ProducerService();
        producerService.send(new TransactionDto()
                .setTransactionId("111")
                .setAmount(555L)
                .setOperationType(OperationType.DEPOSIT)
                .setDate(new Date()));
    }
}
