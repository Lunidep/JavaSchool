package sbp.school.kafka.dto;

import lombok.Getter;

public enum OperationType {
    DEPOSIT("Внесение денежных средств"),
    WITHDRAWAL("Снятие денежных средств");

    @Getter
    private final String meaning;

    OperationType(String meaning) {
        this.meaning = meaning;
    }
}
