package sbp.utils.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import sbp.dto.AckDto;

import java.nio.charset.StandardCharsets;

import static sbp.utils.JsonSchemaValidator.objectMapper;

@Slf4j
public class AckDeserializer implements Deserializer<AckDto> {

    @Override
    public AckDto deserialize(String topic, byte[] data) {
        if (data == null) {
            String errorMessage = "Data cannot be null when deserializing from " + topic;
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        try {
            String value = new String(data, StandardCharsets.UTF_8);
            return objectMapper.readValue(value, AckDto.class);
        } catch (JsonProcessingException e) {
            String errorMessage = "Error deserializing data from topic: " + topic;
            log.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }
}
