package sbp.utils.serializers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import sbp.dto.TransactionDto;
import sbp.utils.JsonSchemaValidator;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

@Slf4j
public class TransactionSerializer implements Serializer<TransactionDto> {

    @Override
    public byte[] serialize(String topic, TransactionDto transactionDto) {
        if (transactionDto == null) {
            String errorMessage = "TransactionDto cannot be null when serializing to topic: " + topic;
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        try {
            JsonSchemaValidator.validateTransaction(transactionDto, Path.of("json-schemas/schema.json"));

            String value = JsonSchemaValidator.objectMapper.writeValueAsString(transactionDto);
            return value.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
