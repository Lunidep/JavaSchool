package sbp.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import sbp.config.TopicPropertiesLoader;
import sbp.constants.Constants;
import sbp.dto.TransactionDto;
import sbp.utils.JsonSchemaValidator;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ConsumerService {
    private static final int MESSAGE_COMMIT_THRESHOLD = 150;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private final String topicName = TopicPropertiesLoader.getTopicProperties().getProperty(Constants.TOPIC_NAME);

    public void read(Properties properties) {
        AtomicInteger messageCounter = new AtomicInteger(0);
        try(KafkaConsumer<String, TransactionDto> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topicName));

            try {
                while (true) {
                    ConsumerRecords<String, TransactionDto> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, TransactionDto> record : records) {
                        JsonSchemaValidator.validateTransaction(record.value(), Path.of(Constants.PATH_TO_SCHEMA));

                        processMessage(properties, record);

                        currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1));

                        commitAsyncOnceAThreshold(messageCounter, consumer);
                        messageCounter.incrementAndGet();
                    }
                }
            }
            catch (Exception e) {
                log.error("Unexpected error", e);
            }
            finally {
                try {
                    consumer.commitSync();
                }
                finally {
                    consumer.close();
                }
            }
        }
    }

    private void commitAsyncOnceAThreshold(AtomicInteger messageCounter, KafkaConsumer<String, TransactionDto> consumer) {
        if (isThresholdPassed(messageCounter)) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if (Objects.nonNull(exception)) {
                    log.error("commit failed for offsets {}", offsets, exception);
                }
            });
        }
    }

    private static void processMessage(Properties properties, ConsumerRecord<String, TransactionDto> record) {
        log.info(String.format("consumer group: %s, topic: %s, partition: %d, key: %s, value: %s, offset: %d",
                properties.get("group.id"),
                record.topic(),
                record.partition(),
                record.key(),
                record.value(),
                record.offset()));
    }

    private static boolean isThresholdPassed(AtomicInteger messageCounter) {
        return messageCounter.get() % MESSAGE_COMMIT_THRESHOLD == 0;
    }
}

