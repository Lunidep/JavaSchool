package sbp.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import sbp.config.TransactionPropertiesLoader;
import sbp.constants.Constants;
import sbp.dto.TransactionDto;
import sbp.storage.ackproducerstorage.TransactionStorage;
import sbp.utils.JsonSchemaValidator;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.nonNull;
import static sbp.constants.Constants.PRODUCER_ID_HEADER_KEY;

@Slf4j
public class ConsumerService extends Thread implements AutoCloseable {
    private Consumer<String, TransactionDto> consumer;

    private static final int MESSAGE_COMMIT_THRESHOLD = 150;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private final String topicName;

    private final TransactionStorage storage;

    public ConsumerService(Properties consumerProperties, TransactionStorage storage) {
        Properties transactionProperties = TransactionPropertiesLoader.getTopicProperties();
        this.topicName = transactionProperties.getProperty("transaction.topic.name");

        this.consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of(topicName));
        consumer.assignment().forEach(partition -> accept(partition, consumer));

        this.storage = storage;
    }

    public void setKafkaConsumer(Consumer<String, TransactionDto> consumer) {
        this.consumer = consumer;
    }

    public void consume() {
        AtomicInteger messageCounter = new AtomicInteger(0);

        try {
            while (true) {
                ConsumerRecords<String, TransactionDto> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, TransactionDto> record : records) {
                    JsonSchemaValidator.validateTransaction(record.value(), Path.of(Constants.PATH_TO_SCHEMA));

                    processMessage(record);

                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));

                    commitAsyncOnceAThreshold(messageCounter, consumer);
                    messageCounter.incrementAndGet();
                }
            }
        } catch (WakeupException e) {
            // Игнорируем для корректного завершения
        } catch (Exception e) {
            log.info("Unexpected error", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    private void accept(TopicPartition partition, Consumer<String, TransactionDto> consumer) {
        var offsetAndMetadata = currentOffsets.get(partition);
        if (nonNull(offsetAndMetadata)) {
            consumer.seek(partition, offsetAndMetadata);
        }
    }

    private void commitAsyncOnceAThreshold(AtomicInteger messageCounter, Consumer<String, TransactionDto> consumer) {
        if (isThresholdPassed(messageCounter)) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if (Objects.nonNull(exception)) {
                    log.info("commit failed for offsets {}", offsets, exception);
                }
            });
        }
    }

    private void processMessage(ConsumerRecord<String, TransactionDto> record) {
        TransactionDto transaction = record.value();
        String producerId = new String(record.headers().lastHeader(PRODUCER_ID_HEADER_KEY).value());
        if (transaction == null || producerId.isBlank()) {
            log.info("Пропущено невалидного сообщение: producerId={}, offset={}", producerId, record.offset());
            return;
        }

        storage.addTransactionByProducer(producerId, transaction);

        log.info("Получена и обработана валидная транзакция: {}, producerId={}, offset={}",
                transaction, producerId, record.offset());
    }

    private static boolean isThresholdPassed(AtomicInteger messageCounter) {
        return messageCounter.get() % MESSAGE_COMMIT_THRESHOLD == 0;
    }

    @Override
    public void run() {
        consume();
    }

    /**
     * Немедленное прерывает вычитку сообщений
     */
    @Override
    public void close() {
        log.info("Прерывание потребителя");
        consumer.wakeup();
    }
}

