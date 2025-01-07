package sbp.school.kafka.service;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sbp.config.TransactionPropertiesLoader;
import sbp.dto.TransactionDto;
import sbp.storage.ackconsumerstorage.ChecksumStorage;
import sbp.storage.ackconsumerstorage.InProcessTransactionsStorage;
import sbp.storage.ackconsumerstorage.RetryCountStorage;
import sbp.storage.ackconsumerstorage.SentTransactionsStorage;
import sbp.storage.ackconsumerstorage.Storage;
import sbp.storage.ackconsumerstorage.impl.ChecksumStorageImpl;
import sbp.storage.ackconsumerstorage.impl.InProcessTransactionsStorageImpl;
import sbp.storage.ackconsumerstorage.impl.RetryCountStorageImpl;
import sbp.storage.ackconsumerstorage.impl.SentTransactionsStorageImpl;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static sbp.constants.Constants.PRODUCER_ID_HEADER_KEY;
import static sbp.utils.IntervalCalculator.getIntervalKey;

@Slf4j
public class ProducerService extends Thread implements AutoCloseable {
    private final KafkaProducer<String, TransactionDto> producer;

    private final ChecksumStorage checksumStorage;
    private final InProcessTransactionsStorage inProcessTransactionsStorage;
    private final RetryCountStorage retryCountStorage;
    private final SentTransactionsStorage sentTransactionsStorage;

    private final String topicName;
    private final Duration ackTimeout;
    private final Duration checksumIntervalDuration;
    private final int retryMaxCount;

    @Getter
    private final UUID producerId;

    public ProducerService(Properties producerProperties, Storage storage) {
        this.producer = new KafkaProducer<>(producerProperties);
        this.producerId = UUID.randomUUID();

        Properties transactionProperties = TransactionPropertiesLoader.getTopicProperties();
        this.topicName = transactionProperties.getProperty("transaction.topic.name");
        this.ackTimeout = Duration.parse(transactionProperties.getProperty("transaction.ack.timeout"));
        this.checksumIntervalDuration = Duration.parse(transactionProperties.getProperty("transaction.checksum.interval"));
        this.retryMaxCount = Integer.parseInt(transactionProperties.getProperty("transaction.retry.maxcount"));

        //storage
        checksumStorage = new ChecksumStorageImpl(storage);
        inProcessTransactionsStorage = new InProcessTransactionsStorageImpl(storage);
        retryCountStorage = new RetryCountStorageImpl(storage);
        sentTransactionsStorage = new SentTransactionsStorageImpl(storage);
    }

    @Override
    public void run() {
        retrySendTransactions();
    }

    public void retrySendTransactions() {
        // Если хранилище отправленных транзакций пусто, выходим из метода
        if (sentTransactionsStorage.isSentTransactionsEmpty()) {
            return;
        }

        log.trace("Проверка необходимости переотправки транзакций");

        LocalDateTime currentTime = LocalDateTime.now();
        LocalDateTime retryThresholdTime = currentTime.minus(ackTimeout); // Время, после которого транзакции считаются устаревшими
        Long retryThresholdIntervalKey = getIntervalKey(retryThresholdTime, checksumIntervalDuration);

        // Получаем номера интервалов, которые требуют переотправки
        Set<Long> intervalsToRetry = sentTransactionsStorage.getSentTransactionIntervalKeys().stream()
                .filter(intervalKey -> intervalKey < retryThresholdIntervalKey)
                .collect(Collectors.toSet());

        // Переотправляем транзакции для каждого интервала
        intervalsToRetry.forEach(intervalKey -> {
            int retriedTransactionsCount = retryTransactionsForInterval(intervalKey, currentTime);
            if (retriedTransactionsCount > 0) {
                log.info("Переотправка транзакций выполнена для интервала: intervalKey={}", intervalKey);
            }
            sentTransactionsStorage.cleanupInterval(intervalKey); // Очищаем данные по интервалу после переотправки
        });
    }

    private int retryTransactionsForInterval(Long intervalKey, LocalDateTime currentTime) {
        // Получаем список транзакций для указанного интервала
        List<TransactionDto> transactions = sentTransactionsStorage.getSentTransactions(intervalKey);

        // Счетчик успешно переотправленных транзакций
        AtomicInteger transactionsSentCount = new AtomicInteger(0);

        transactions.stream()
                .filter(transaction -> {
                    String transactionId = transaction.getTransactionId();
                    int retryCount = retryCountStorage.getRetryCount(transactionId);

                    if (retryCount >= retryMaxCount) {
                        log.info("Превышено максимальное количество попыток переотправки для транзакции: id={}, retryCount={}",
                                transactionId, retryCount);
                        return false;
                    }
                    return true;
                })
                .forEach(transaction -> {
                    String transactionId = transaction.getTransactionId();
                    int retryCount = retryCountStorage.getRetryCount(transactionId);

                    retryCountStorage.putRetryCount(transactionId, retryCount + 1);

                    // Создаем транзакцию для переотправки
                    TransactionDto retryTransaction = createRetryTransaction(transaction, currentTime);
                    send(retryTransaction);

                    // Увеличиваем счетчик успешно переотправленных транзакций
                    transactionsSentCount.incrementAndGet();
                });

        // Возвращаем количество успешно переотправленных транзакций
        return transactionsSentCount.get();
    }

    public void send(TransactionDto transaction) {
        log.info("Начинаем отправку транзакции {} в топик {}", transaction, topicName);
        ProducerRecord<String, TransactionDto> record = new ProducerRecord<>(topicName, transaction.getOperationType().name(), transaction);
        record.headers().add(PRODUCER_ID_HEADER_KEY, producerId.toString().getBytes());
        inProcessTransactionsStorage.putTransactionSendInProgress(transaction);
        try {
            producer.send(record, (metadata, exception) -> {
                onCompletion(transaction, metadata, exception);
            });
            producer.flush();
        } catch (Throwable ex) {
            log.error("Не удалось отправить транзакцию {} в топик {}. Причина: {}", transaction, topicName, ex.getMessage());
            producer.flush();
        }
    }

    private void onCompletion(TransactionDto transaction, RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            long intervalKey = getIntervalKey(transaction.getDate(), checksumIntervalDuration);
            sentTransactionsStorage.putSentTransaction(intervalKey, transaction);
            checksumStorage.updateCheckSum(intervalKey);
            log.info("Транзакция успешно отправлена: id={}, intervalKey={}, checkSum={}, partition={}, offset={}",
                    transaction.getTransactionId(), intervalKey, checksumStorage.getSentCheckSum(intervalKey), metadata.partition(), metadata.offset());
            inProcessTransactionsStorage.removeTransactionSendInProgress(transaction.getTransactionId());
        } else {
            log.error("Ошибка при отправке: {}. Данные: offset = {}, partition = {}",
                    exception.getMessage(), metadata.offset(), metadata.partition());
        }
    }

    private TransactionDto createRetryTransaction(TransactionDto original, LocalDateTime time) {
        return new TransactionDto(
                original.getTransactionId(),
                original.getOperationType(),
                original.getAmount(),
                time
        );
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
