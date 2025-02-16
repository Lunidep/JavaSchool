package sbp.school.kafka.ack.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import sbp.config.TransactionPropertiesLoader;
import sbp.dto.AckDto;
import sbp.storage.Storage;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.nonNull;

@Slf4j
public class AckConsumerService extends Thread implements AutoCloseable {
    private static final int MESSAGE_COMMIT_THRESHOLD = 150;

    private final KafkaConsumer<String, AckDto> consumer;

    private final String topicName;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    private final MessageProcessor messageProcessor;

    public AckConsumerService(Properties consumerProperties, Storage storage) {
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.messageProcessor = new MessageProcessor(storage);

        this.topicName = TransactionPropertiesLoader.getTopicProperties().getProperty("transaction.ack.topic.name");
        this.currentOffsets = new HashMap<>();

    }

    public void read() {
        AtomicInteger messageCounter = new AtomicInteger(0);
        consumer.subscribe(List.of(topicName));
        consumer.assignment().forEach(partition -> accept(partition, consumer));

        try {
            while (true) {
                ConsumerRecords<String, AckDto> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, AckDto> record : records) {
                    // Обрабатываем сообщение
                    messageProcessor.processMessage(record);

                    // Обновляем текущий offset для раздела
                    currentOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)
                    );

                    // Асинхронно коммитим offsets, если достигнут порог
                    commitAsyncOnceAThreshold(messageCounter, consumer);
                    messageCounter.incrementAndGet();
                }
            }
        } catch (WakeupException e) {
            // Игнорируем WakeupException для корректного завершения работы
            log.debug("Получен WakeupException, завершение работы потребителя");
        } catch (Exception e) {
            log.error("Неожиданная ошибка при чтении сообщений", e);
            throw new RuntimeException("Ошибка потребителя", e);
        } finally {
            try {
                if (!currentOffsets.isEmpty()) {
                    consumer.commitSync(currentOffsets);
                }
            } finally {
                log.info("Закрытие потребителя");
                currentOffsets.clear();
                consumer.close();
            }
        }
    }

    private void accept(TopicPartition partition, KafkaConsumer<String, AckDto> consumer) {
        var offsetAndMetadata = currentOffsets.get(partition);
        if (nonNull(offsetAndMetadata)) {
            consumer.seek(partition, offsetAndMetadata);
        }
    }

    private void commitAsyncOnceAThreshold(AtomicInteger messageCounter, KafkaConsumer<String, AckDto> consumer) {
        if (isThresholdPassed(messageCounter)) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if (Objects.nonNull(exception)) {
                    log.error("commit failed for offsets {}", offsets, exception);
                }
            });
        }
    }

    private static boolean isThresholdPassed(AtomicInteger messageCounter) {
        return messageCounter.get() % MESSAGE_COMMIT_THRESHOLD == 0;
    }

    @Override
    public void run() {
        read();
    }

    @Override
    public void close() throws Exception {
        consumer.wakeup();
    }

}

