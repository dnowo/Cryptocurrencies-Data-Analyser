package pl.rpd.projekt.spark;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.sql.Timestamp;

@Setter
@Getter
@ToString
public class MiniCryptoDto {
    private Timestamp date;
    private BigDecimal open;
    private BigDecimal close;
}
