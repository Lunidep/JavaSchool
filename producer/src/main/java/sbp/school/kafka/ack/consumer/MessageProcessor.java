package sbp.school.kafka.ack.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import sbp.dto.AckDto;
import sbp.storage.ChecksumStorage;
import sbp.storage.SentTransactionsStorage;
import sbp.storage.Storage;
import sbp.storage.impl.ChecksumStorageImpl;
import sbp.storage.impl.SentTransactionsStorageImpl;

@Slf4j
public class MessageProcessor {

    private final ChecksumStorage checksumStorage;
    private final SentTransactionsStorage sentTransactionsStorage;

    public MessageProcessor(Storage storage) {
        this.checksumStorage = new ChecksumStorageImpl(storage);
        this.sentTransactionsStorage = new SentTransactionsStorageImpl(storage);
    }

    public void processMessage(ConsumerRecord<String, AckDto> record) {
        AckDto ack = record.value();

        // Пытаемся извлечь и проверить ключ интервала
        Long intervalKey = parseIntervalKey(ack.getIntervalKey(), record.offset());
        if (intervalKey == null) {
            return;
        }

        // Получаем контрольные суммы
        String ackChecksum = ack.getChecksum();
        String sentChecksum = checksumStorage.getSentCheckSum(intervalKey);

        // Обрабатываем подтверждение в зависимости от условий
        if (isChecksumValid(ackChecksum, sentChecksum, intervalKey, record.offset())) {
            sentTransactionsStorage.cleanupInterval(intervalKey);
            log.info("Подтверждение успешно обработано. intervalKey={}, offset={}", intervalKey, record.offset());
        }
    }

    /**
     * Пытается преобразовать ключ интервала в число.
     * Если ключ некорректен, логирует ошибку и возвращает null.
     */
    private Long parseIntervalKey(String intervalKeyStr, long offset) {
        try {
            return Long.parseLong(intervalKeyStr);
        } catch (NumberFormatException e) {
            log.info("Подтверждение пропущено: некорректный ключ интервала. intervalKey={}, offset={}",
                    intervalKeyStr, offset);
            return null;
        }
    }

    /**
     * Проверяет валидность контрольной суммы и логирует соответствующие сообщения.
     * Возвращает true, если контрольные суммы совпадают, иначе false.
     */
    private boolean isChecksumValid(String ackChecksum, String sentChecksum, long intervalKey, long offset) {
        if (sentChecksum == null) {
            log.info("Подтверждение получено, но интервал отсутствует в хранилище. " +
                            "ackChecksum={}, sentChecksum=null, intervalKey={}, offset={}",
                    ackChecksum, intervalKey, offset);
            return false;
        }

        if (!ackChecksum.equals(sentChecksum)) {
            log.info("Подтверждение получено, но контрольные суммы не совпадают. " +
                            "ackChecksum={}, sentChecksum={}, intervalKey={}, offset={}",
                    ackChecksum, sentChecksum, intervalKey, offset);
            return false;
        }

        return true;
    }
}
