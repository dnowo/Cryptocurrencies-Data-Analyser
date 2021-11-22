package pl.rpd.projekt.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.*;

import java.math.BigDecimal;
import java.util.Date;

@ToString
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@JsonPropertyOrder({"serialNumber","name","symbol","date","high","low","open","close","volume","marketCap"})
public class Cryptocurrency {
    public Integer serialNumber;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    public Date date;
    public BigDecimal open;
    public BigDecimal close;
    public BigDecimal high;
    public BigDecimal low;
    public BigDecimal volume;
    public BigDecimal marketCap;
    public String name;
    public String symbol;
}