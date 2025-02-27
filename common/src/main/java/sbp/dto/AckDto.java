package sbp.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public final class AckDto {
    private final String intervalKey;
    private final String checksum;

    @JsonCreator
    public AckDto(@JsonProperty("intervalKey") String intervalKey, @JsonProperty("checksum") String checksum) {
        this.intervalKey = intervalKey;
        this.checksum = checksum;
    }
}
