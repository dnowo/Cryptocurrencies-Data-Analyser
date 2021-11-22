package pl.rpd.projekt.model;

import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CryptocurrencyDto {
    public Integer serialNumber;
    public Instant date;
    public BigDecimal open;
    public BigDecimal close;
    public BigDecimal high;
    public BigDecimal low;
    public BigDecimal volume;
    public BigDecimal marketCap;
    public String name;
    public String symbol;
}
