package sbp.school.kafka.dto;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Date;

@Data
@Accessors(chain = true)
public class TransactionDto {
    private OperationType operationType;
    private Long amount;
    private String transactionId;
    private Date date;
}
