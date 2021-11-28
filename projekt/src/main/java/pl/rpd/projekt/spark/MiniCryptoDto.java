package pl.rpd.projekt.spark;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;

@Setter
@Getter
@ToString
public class MiniCryptoDto {
    public Timestamp date;
    public BigDecimal open;
    public BigDecimal close;
}
