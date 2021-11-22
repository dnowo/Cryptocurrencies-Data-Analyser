package pl.rpd.projekt.cassandra;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pl.rpd.projekt.model.Cryptocurrency;
import pl.rpd.projekt.model.CryptocurrencyDto;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class CryptoRepository {
    public static final String TABLE_NAME = "cryptocurrencies";
    private final CassandraConnector cassandraConnector;
    public final static String KEYSPACE = "crypto";

    @Getter
    private Session session;

    @PostConstruct
    void setup() {
        this.connect();
    }

    private Session connect() {
        final String host = "127.0.0.1";
        final Integer port = 9042;
        cassandraConnector.connect(host, port);
        cassandraConnector.createKeyspace(KEYSPACE, "SimpleStrategy", 1);
        this.session = cassandraConnector.getSession();

        ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces;");
        var list = result.all().stream()
                .filter(r -> r.getString(0).equals(KEYSPACE.toLowerCase()))
                .map(r -> r.getString(0))
                .collect(Collectors.toList());
        System.out.println(list.toString());
        createTableCryptocurrency();
        return session;
    }

    private void createTableCryptocurrency() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS " + KEYSPACE + "." + TABLE_NAME + " (")
                .append("serialNumber int PRIMARY KEY,")
                .append("open decimal,")
                .append("date timestamp,")
                .append("close decimal,")
                .append("high decimal,")
                .append("low decimal,")
                .append("volume decimal,")
                .append("marketCap decimal,")
                .append("name text,")
                .append("symbol text,")
                .append(");");
        session.execute(sb.toString());
    }

    public void select() {
        ResultSet result = session.execute(
                "SELECT * FROM " + KEYSPACE + "." + TABLE_NAME + ";");
        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(ColumnDefinitions.Definition::getName)
                        .collect(Collectors.toList());
        System.out.println(columnNames);
    }

    public void insert(Cryptocurrency dto) {
        StringBuilder sb = new StringBuilder("INSERT INTO " + KEYSPACE + "." + TABLE_NAME + " ")
                .append("(serialNumber, open, date, close, high, low, volume, marketCap, name, symbol) VALUES (")
                .append(dto.getSerialNumber() + ",")
                .append(dto.getOpen() + ",")
                .append("'" + dto.getDate().toInstant() + "',")
                .append(dto.getClose() + ",")
                .append(dto.getHigh() + ",")
                .append(dto.getLow() + ",")
                .append(dto.getVolume() + ",")
                .append(dto.getMarketCap() + ",")
                .append("'" +dto.getName() + "',")
                .append("'" +dto.getSymbol() + "'")
                .append(");");

        session.execute(sb.toString());
    }
}
