package sbp.ack.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sbp.config.TransactionPropertiesLoader;
import sbp.dto.AckDto;
import sbp.dto.TransactionDto;
import sbp.storage.ackproducerstorage.TransactionStorage;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static sbp.constants.Constants.PRODUCER_ID_HEADER_KEY;
import static sbp.utils.ChecksumCalculator.calculateChecksum;
import static sbp.utils.IntervalCalculator.getIntervalKey;

/**
 * Класс отправки подтверждений обработки транзакций в брокер сообщений
 */
@Slf4j
public class AckProducerService extends Thread implements AutoCloseable  {
    private static final Logger logger = LoggerFactory.getLogger(AckProducerService.class);

    private final String topicName;

    private final KafkaProducer<String, AckDto> producer;

    private final TransactionStorage storage;

    private final Duration receiveTimeout;

    private final Duration checksumIntervalDuration;

    public AckProducerService(Properties producerProperties, TransactionStorage storage) {
        this.producer = new KafkaProducer<>(producerProperties);
        this.storage = storage;

        Properties transactionProperties = TransactionPropertiesLoader.getTopicProperties();
        this.topicName = transactionProperties.getProperty("transaction.ack.topic.name");
        this.receiveTimeout = Duration.parse(transactionProperties.getProperty("transaction.receive.timeout"));
        this.checksumIntervalDuration = Duration.parse(transactionProperties.getProperty("transaction.checksum.interval"));
    }

    public void sendAck() {
        if (storage.isEmpty()) {
            log.info("Нет полученных транзакций для обработки.");
            return;
        }
        log.info("Начало отправки подтверждений.");

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime timeoutThresholdTime = now.minus(receiveTimeout);
        long timeoutThresholdIntervalKey = getIntervalKey(timeoutThresholdTime, checksumIntervalDuration);

        Set<String> producerIds = storage.getProducerIds();
        int ackCounter = producerIds.stream()
                .mapToInt(producerId -> processProducerTransactions(producerId, timeoutThresholdIntervalKey))
                .sum();

        if (ackCounter == 0) {
            log.info("Нет подтверждений, готовых для отправки.");
        }
    }

    private int processProducerTransactions(String producerId, long timeoutThresholdIntervalKey) {
        List<TransactionDto> processedTransactions = storage.getTransactionsByProducer(producerId);
        Map<Long, List<String>> transactionIdsToAck = groupTransactionsByIntervalKey(processedTransactions, timeoutThresholdIntervalKey);

        transactionIdsToAck.forEach((intervalKey, transactionIds) -> {
            sendAck(producerId, intervalKey, transactionIds);
        });

        return transactionIdsToAck.size();
    }

    private Map<Long, List<String>> groupTransactionsByIntervalKey(List<TransactionDto> transactions, long timeoutThresholdIntervalKey) {
        return transactions.stream()
                .filter(transaction -> getIntervalKey(transaction.getDate(), checksumIntervalDuration) < timeoutThresholdIntervalKey)
                .collect(Collectors.groupingBy(
                        transaction -> getIntervalKey(transaction.getDate(), checksumIntervalDuration),
                        Collectors.mapping(TransactionDto::getTransactionId, Collectors.toList())
                ));
    }

    private void sendAck(String producerId, Long intervalKey, List<String> transactionIds) {
        String checkSum = calculateChecksum(transactionIds);
        AckDto ack = new AckDto(intervalKey.toString(), checkSum);
        ProducerRecord<String, AckDto> record = new ProducerRecord<>(topicName, ack);
        record.headers().add(PRODUCER_ID_HEADER_KEY, producerId.getBytes());

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Ошибка при отправке подтверждения: partition={}, offset={}", metadata.partition(), metadata.offset(), exception);
            } else {
                storage.removeTransactions(producerId, transactionIds);
                log.info("Подтверждение успешно отправлено: producerId={}, intervalKey={}, checkSum={}, partition={}, offset={}",
                        producerId, ack.getIntervalKey(), ack.getChecksum(), metadata.partition(), metadata.offset());
            }
        });
    }
    @Override
    public void run() {
        sendAck();
    }

    /**
     * Завершает все операции, освобождает ресурсы
     */
    @Override
    public void close() {
        logger.info("Отправка неотправленных подтверждений и закрытие производителя подтверждений");
        producer.close();
    }
}
