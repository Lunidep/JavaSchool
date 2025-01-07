package sbp.school.kafka;

import sbp.config.KafkaProducerPropertiesLoader;
import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.school.kafka.service.ProducerService;
import sbp.storage.Storage;

import java.time.LocalDateTime;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Storage storage = new Storage();
        Properties producerProperties = KafkaProducerPropertiesLoader.getKafkaProducerProperties();
        ProducerService producerService = new ProducerService(producerProperties, storage);
        producerService.send(new TransactionDto()
                .setTransactionId("111")
                .setAmount(555L)
                .setOperationType(OperationType.DEPOSIT)
                .setDate(LocalDateTime.now()));
    }
}
