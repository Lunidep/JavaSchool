package sbp.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Коннектор для чтения транзакций из базы данных и отправки их в Kafka.
 * Этот коннектор читает данные из указанной базы данных и публикует их в указанный топик Kafka.
 */
public class TransactionDatabaseSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(TransactionDatabaseSourceConnector.class);

    /**
     * Название конфигурации для указания топика Kafka.
     */
    public static final String TOPIC_CONFIG = "topic";

    /**
     * Название конфигурации для указания базы данных.
     */
    public static final String DB_CONFIG = "database";

    /**
     * Название конфигурации для указания размера пакета данных, обрабатываемого за один раз.
     */
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

    /**
     * Значение по умолчанию для размера пакета данных.
     */
    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    /**
     * Определение конфигураций коннектора.
     */
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DB_CONFIG, ConfigDef.Type.STRING, "transactions", ConfigDef.Importance.HIGH,
                    "Исходная база данных")
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH,
                    "Топик, в который публикуются данные")
            .define(TASK_BATCH_SIZE_CONFIG, ConfigDef.Type.INT, DEFAULT_TASK_BATCH_SIZE, ConfigDef.Importance.LOW,
                    "Максимальное количество записей, которое задача может прочитать за один вызов метода poll");

    private Map<String, String> props;

    /**
     * Запуск коннектора. Инициализирует конфигурацию и логирует начало работы.
     *
     * @param props Конфигурационные свойства коннектора.
     */
    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        log.info("Запуск коннектора для чтения из базы данных: {}", new AbstractConfig(CONFIG_DEF, props).getString(DB_CONFIG));
    }

    /**
     * Возвращает класс задачи, которая будет использоваться для обработки данных.
     *
     * @return Класс задачи.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return TransactionDatabaseSourceTask.class;
    }

    /**
     * Генерирует конфигурации для задач. Каждая задача получает одинаковую конфигурацию.
     *
     * @param maxTasks Максимальное количество задач.
     * @return Список конфигураций для задач.
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    /**
     * Остановка коннектора. В текущей реализации не выполняет никаких действий.
     */
    @Override
    public void stop() {
        log.info("Остановка коннектора");
    }

    /**
     * Возвращает определение конфигурации коннектора.
     *
     * @return Определение конфигурации.
     */
    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    /**
     * Возвращает версию коннектора.
     *
     * @return Версия коннектора.
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }
}