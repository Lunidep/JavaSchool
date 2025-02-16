package sbp.utils.serializers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import sbp.dto.AckDto;
import sbp.utils.JsonSchemaValidator;

import java.nio.charset.StandardCharsets;

@Slf4j
public class AckSerializer implements Serializer<AckDto> {

    @Override
    public byte[] serialize(String topic, AckDto ackDto) {
        if (ackDto == null) {
            String errorMessage = "AckDto cannot be null when serializing to topic: " + topic;
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }

        try {
            String value = JsonSchemaValidator.objectMapper.writeValueAsString(ackDto);
            return value.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
