package sbp.school.kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import sbp.school.kafka.config.KafkaProducerPropertiesLoader;
import sbp.school.kafka.config.TopicPropertiesLoader;
import sbp.school.kafka.constants.Constants;
import sbp.school.kafka.dto.TransactionDto;

@Slf4j
public class ProducerService {

    private final KafkaProducer<String, TransactionDto> producer;
    private final String topic;

    public ProducerService() {
        this.producer = new KafkaProducer<>(KafkaProducerPropertiesLoader.getKafkaProducerProperties());
        this.topic = TopicPropertiesLoader.getTopicProperties().getProperty(Constants.TOPIC_NAME);
    }

    public void send(TransactionDto transaction) {
        log.info("Начинаем отправку транзакции {} в топик {}", transaction, topic);
        try {
            producer.send(new ProducerRecord<>(topic, transaction.getOperationType().name(), transaction),
                    ProducerService::onCompletion);
        } catch (Throwable ex) {
            log.error("Не удалось отправить транзакцию {} в топик {}. Причина: {}", transaction, topic, ex.getMessage());
            producer.flush();
        }
    }

    private static void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        if (exception == null) {
            log.debug("Успешно отправлено: offset = {}, partition = {}",
                    recordMetadata.offset(), recordMetadata.partition());
        } else {
            log.error("Ошибка при отправке: {}. Данные: offset = {}, partition = {}",
                    exception.getMessage(), recordMetadata.offset(), recordMetadata.partition());
        }
    }
}
