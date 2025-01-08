package sbp;

import sbp.config.KafkaConfig;
import sbp.service.ConsumerService;

public class Main {
    public static void main(String[] args) {
        ConsumerService consumerService = new ConsumerService(KafkaConfig.getKafkaProperties());
        consumerService.read();
    }
}