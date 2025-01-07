package sbp.school.kafka.dto;

import lombok.Getter;

@Getter
public enum OperationType {
    DEPOSIT("Внесение денежных средств"),
    WITHDRAWAL("Снятие денежных средств");

    private final String meaning;

    OperationType(String meaning) {
        this.meaning = meaning;
    }
}
