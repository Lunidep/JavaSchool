package sbp.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Сервис для работы с базой данных.
 * Отвечает за выполнение SQL-запросов и извлечение данных.
 */
public class DatabaseService {

    private static final Logger log = LoggerFactory.getLogger(DatabaseService.class);
    private final String dbUrl;

    /**
     * Конструктор для инициализации сервиса.
     *
     * @param dbUrl URL базы данных.
     */
    public DatabaseService(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    /**
     * Получает все транзакции из базы данных, начиная с указанного смещения.
     *
     * @param skip Смещение (количество записей, которые нужно пропустить).
     * @return Список транзакций в формате JSON.
     */
    public List<String> getAllTransactions(long skip) {
        List<String> transactions = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(dbUrl);
             PreparedStatement stat = conn.prepareStatement("SELECT * FROM transactionKafka OFFSET ?")) {
            stat.setLong(1, skip);
            try (ResultSet rs = stat.executeQuery()) {
                while (rs.next()) {
                    transactions.add(rs.getString("transactionJson"));
                }
            }
        } catch (Exception e) {
            log.error("Ошибка при выполнении запроса к базе данных", e);
            throw new RuntimeException("Ошибка при работе с базой данных", e);
        }
        return transactions;
    }
}