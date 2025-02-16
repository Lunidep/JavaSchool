package sbp.utils.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.dto.TransactionDto;

import java.nio.charset.StandardCharsets;

@Slf4j
public class TransactionDeserializer implements Deserializer<TransactionDto> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TransactionDto deserialize(String topic, byte[] data) {
        if (data == null) {
            String errorMessage = "Data cannot be null when deserializing from " + topic;
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        try {
            String value = new String(data, StandardCharsets.UTF_8);
            return objectMapper.readValue(value, TransactionDto.class);
        } catch (JsonProcessingException e) {
            String errorMessage = "Error deserializing data from topic: " + topic;
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }
}
