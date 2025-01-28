package sbp.school.kafka;

import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.config.KafkaProducerPropertiesLoader;
import sbp.school.kafka.service.ProducerService;

import java.util.Date;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties producerProperties = KafkaProducerPropertiesLoader.getKafkaProducerProperties();
        ProducerService producerService = new ProducerService(producerProperties);
        producerService.send(new TransactionDto()
                .setTransactionId("111")
                .setAmount(555L)
                .setOperationType(OperationType.DEPOSIT)
                .setDate(new Date()));
    }
}
