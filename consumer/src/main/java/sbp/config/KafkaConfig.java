package sbp.config;

import java.util.Properties;

public class KafkaConfig {
    public static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "consumer-1");
        return props;
    }
}
