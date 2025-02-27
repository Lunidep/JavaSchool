package sbp.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Задача для чтения транзакций из базы данных и отправки их в Kafka.
 */
public class TransactionDatabaseSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(TransactionDatabaseSourceTask.class);
    private static final String DATABASE_NAME_FIELD = "dbname";
    private static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String databaseName;
    private String topic;
    private Long dbOffset;
    private DatabaseService databaseService;

    /**
     * Инициализация задачи.
     *
     * @param props Конфигурационные свойства задачи.
     */
    @Override
    public void start(Map<String, String> props) {
        AbstractConfig config = new AbstractConfig(TransactionDatabaseSourceConnector.CONFIG_DEF, props);
        databaseName = config.getString(TransactionDatabaseSourceConnector.DB_CONFIG);
        topic = config.getString(TransactionDatabaseSourceConnector.TOPIC_CONFIG);

        DatabaseConfig dbConfig = new DatabaseConfig("application.properties");
        databaseService = new DatabaseService(dbConfig.getDbUrl());
    }

    /**
     * Получение данных из базы данных и преобразование их в записи Kafka.
     *
     * @return Список записей для отправки в Kafka.
     */
    @Override
    public List<SourceRecord> poll() {
        Map<String, Object> offset = context.offsetStorageReader()
                .offset(Collections.singletonMap(DATABASE_NAME_FIELD, databaseName));
        dbOffset = (offset != null && offset.get(POSITION_FIELD) instanceof Long)
                ? (Long) offset.get(POSITION_FIELD)
                : 0L;

        List<String> transactions = databaseService.getAllTransactions(dbOffset);
        return transactions.stream()
                .map(transaction -> new SourceRecord(
                        offsetKey(databaseName),
                        offsetValue(dbOffset),
                        topic,
                        null,
                        null,
                        null,
                        VALUE_SCHEMA,
                        transaction,
                        System.currentTimeMillis()
                ))
                .toList();
    }

    /**
     * Остановка задачи.
     */
    @Override
    public void stop() {
        log.info("Задача остановлена");
    }

    /**
     * Возвращает версию задачи.
     *
     * @return Версия задачи.
     */
    @Override
    public String version() {
        return new TransactionDatabaseSourceConnector().version();
    }

    private Map<String, String> offsetKey(String databaseName) {
        return Collections.singletonMap(DATABASE_NAME_FIELD, databaseName);
    }

    private Map<String, Long> offsetValue(Long position) {
        return Collections.singletonMap(POSITION_FIELD, position);
    }
}