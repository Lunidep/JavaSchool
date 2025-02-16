package sbp.school.kafka.utils.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import sbp.school.kafka.dto.OperationType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class TransactionPartitioner implements Partitioner {
    private static final Map<String, Integer> KEY_PARTITION = Arrays.stream(OperationType.values())
            .collect(Collectors.toMap(OperationType::name, OperationType::ordinal));
    private static final int REQUIRED_NUMBER_OF_PARTITIONS = KEY_PARTITION.size();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        validateKey(key, keyBytes);

        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        validatePartitionSize(partitionInfos.size());

        return getPartitionForKey(key);
    }

    private void validateKey(Object key, byte[] keyBytes) {
        if (keyBytes == null || !(key instanceof String)) {
            String errorMessage = "Ошибка: ключ должен быть строкой. Убедитесь, что переданный ключ соответствует ожидаемому типу.";
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private void validatePartitionSize(int size) {
        if (size < REQUIRED_NUMBER_OF_PARTITIONS) {
            String message = String.format("Ошибка: количество партиций должно быть не менее %d. Текущее значение: %d.",
                    REQUIRED_NUMBER_OF_PARTITIONS, size);
            log.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    private int getPartitionForKey(Object key) {
        Integer partition = KEY_PARTITION.get(key);
        if (partition == null) {
            String message = String.format("Ошибка: ключа '%s'. Не удалось определить соответствующую партицию.", key);
            log.error(message);
            throw new IllegalArgumentException(message);
        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
