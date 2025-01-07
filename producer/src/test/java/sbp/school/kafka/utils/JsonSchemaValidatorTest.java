package sbp.school.kafka.utils;

import org.junit.jupiter.api.Test;
import sbp.dto.OperationType;
import sbp.dto.TransactionDto;
import sbp.utils.JsonSchemaValidator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonSchemaValidatorTest {
    private final Path schemaPath = Paths.get("json-schemas/schema.json");

    @Test
    void testValidateTransactionSuccess() {
        TransactionDto validTransaction = new TransactionDto()
                .setTransactionId("111")
                .setAmount(555L)
                .setOperationType(OperationType.DEPOSIT)
                .setDate(LocalDateTime.now());

        assertDoesNotThrow(() -> JsonSchemaValidator.validateTransaction(validTransaction, schemaPath));
    }

    @Test
    void testValidateTransactionFailure() {
        TransactionDto invalidTransaction = new TransactionDto()
                .setTransactionId("111")
                .setAmount(555L)
                .setOperationType(null)
                .setDate(LocalDateTime.now());

        assertThrows(Exception.class, () ->
                JsonSchemaValidator.validateTransaction(invalidTransaction, schemaPath)
        );
    }
}