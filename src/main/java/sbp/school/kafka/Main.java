package sbp.school.kafka;

import sbp.school.kafka.service.ProducerService;

public class Main {
    public static void main(String[] args) {
        ProducerService producerService = new ProducerService();
        producerService.send();
    }
}
