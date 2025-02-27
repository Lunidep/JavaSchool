package sbp.source;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.ConfigurationException;


/**
 * Класс для работы с конфигурацией базы данных.
 */
public class DatabaseConfig {

    private final PropertiesConfiguration config;

    /**
     * Конструктор для загрузки конфигурации из файла.
     *
     * @param configFilePath Путь к файлу конфигурации.
     */
    public DatabaseConfig(String configFilePath) {
        this.config = new PropertiesConfiguration();
        try {
            config.load(configFilePath);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Не удалось загрузить конфигурационный файл: " + configFilePath, e);
        }
    }

    /**
     * Возвращает URL базы данных.
     *
     * @return URL базы данных.
     */
    public String getDbUrl() {
        return config.getString("dbname");
    }
}