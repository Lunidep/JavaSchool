package sbp.school.kafka.utils;

import org.junit.jupiter.api.Test;
import sbp.school.kafka.dto.OperationType;
import sbp.school.kafka.dto.TransactionDto;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class JsonSchemaValidatorTest {
    private final Path schemaPath = Paths.get("json-schemas/schema.json");

    @Test
    void testValidateTransactionSuccess() {
        TransactionDto validTransaction = new TransactionDto()
                .setTransactionId("111")
                .setAmount(555L)
                .setOperationType(OperationType.DEPOSIT)
                .setDate(new Date());

        assertDoesNotThrow(() -> JsonSchemaValidator.validateTransaction(validTransaction, schemaPath));
    }

    @Test
    void testValidateTransactionFailure() {
        TransactionDto invalidTransaction = new TransactionDto()
                .setTransactionId("111")
                .setAmount(555L)
                .setOperationType(null)
                .setDate(new Date());

        assertThrows(Exception.class, () ->
                JsonSchemaValidator.validateTransaction(invalidTransaction, schemaPath)
        );
    }
}