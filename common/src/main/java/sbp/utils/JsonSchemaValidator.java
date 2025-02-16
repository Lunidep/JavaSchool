package sbp.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import sbp.dto.TransactionDto;

import java.io.InputStream;
import java.nio.file.Path;

@Slf4j
public class JsonSchemaValidator {
    public static ObjectMapper objectMapper;

    static  {
        objectMapper = new ObjectMapper();
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.registerModule(new JavaTimeModule());
    }

    public static void validateTransaction(TransactionDto transaction, Path schemaPath) throws Exception {
        JsonNode jsonNode = objectMapper.valueToTree(transaction);

        try (InputStream schemaStream = inputStreamFromClasspath(schemaPath.toString())) {
            JSONObject jsonSchema = new JSONObject(new JSONTokener(schemaStream));
            Schema schema = SchemaLoader.load(jsonSchema);

            schema.validate(new JSONObject(jsonNode.toString()));

        } catch (SchemaException e) {
            log.error("Validation error: {}", e.getMessage());
        }
    }

    private static InputStream inputStreamFromClasspath(String path) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
    }
}
