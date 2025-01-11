package sbp.school.kafka;

import static org.junit.jupiter.api.Assertions.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaDuplicateMessageTest {

    private static final String TOPIC = "transactions-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static KafkaProducer<String, String> producer;
    private static long startTime = 0L;
    private static long endTime = 0L;

    @BeforeAll
    public static void setup() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);
    }

    @Test
    public void testDuplicateMessageConsumption() throws InterruptedException {
        String message = "LOL MESSAGE";
        producer.send(new ProducerRecord<>(TOPIC, message));
        producer.flush();
        AtomicInteger count = new AtomicInteger(0);

        startTime = System.nanoTime();
        // создание консьюмера
        KafkaConsumer<String, String> consumer1 = consumerGetMessage("CONSUMER 1 ", count);
        // удаление консьюмера (он действительно удаляется, отработал finalize)
        consumer1 = null;
        System.gc();

        endTime = System.nanoTime();
        long durationInMillis = (endTime - startTime) / 1_000_000; // переводим в миллисекунды
        System.out.println("Время от создания консьюмера до удаления: " + durationInMillis + " миллисекунд\n" +
                "Время на которое настроен автокоммит: 1000000000 миллисекунд");

        // создаю consumer2  той же консьюмер-группе, по идее первый консьюмер не мог успеть автозакоммитить то что он считал
        KafkaConsumer<String, String> consumer2 = consumerGetMessage("CONSUMER 2 ", count);
        consumer2.close();

        // ожидаю обработать одно сообщение отправленное продьюсером 2 раза - в обоих консьюмерах
        // потому что автокоммит очень редкий и по идее когда в консьюмер-группу в которой был consumer1 я добавил consumer2
        // последние коммиты примениться не должны были и данные должны быть считанны повторно
        assertThat(count.intValue()).isGreaterThan(1);
    }

    private static KafkaConsumer<String, String> consumerGetMessage(String name, AtomicInteger count) {
        try(KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties()) {
            @Override
            protected void finalize() throws Throwable {
                try {
                    System.out.println("Объект удаляется");
                } finally {
                    super.finalize();
                }
            }
        }) {
            consumer.subscribe(List.of(TOPIC));

            while (true) {
                // второй консьюмер застревает навечно в этом while (true)
                // здравый смысл говорит, что коммит первого консьюмера где-то как-то происходит но я не понимаю где,
                // завершается он аварийно, его сносит gc =)
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100L));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(name + record.value());
                    count.incrementAndGet();
                }
                if (!records.isEmpty()) {
                    break;
                }
            }
            return consumer;
        }
    }

    // у обоих консьюмеров разумеется одинаковые properties
    private static Properties getConsumerProperties() {
        Properties consumerProps1 = new Properties();
        consumerProps1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps1.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps1.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps1.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps1.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps1.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000000000");
        consumerProps1.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return consumerProps1;
    }

    @AfterAll
    public static void tearDown() {
        producer.close();
    }
}
