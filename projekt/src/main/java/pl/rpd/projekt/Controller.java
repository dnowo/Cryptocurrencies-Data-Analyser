package pl.rpd.projekt;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import pl.rpd.projekt.model.Cryptocurrency;
import pl.rpd.projekt.producer.Producer;
import pl.rpd.projekt.spark.ApacheSpark;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.LinkedList;

@RestController
@RequiredArgsConstructor
public class Controller {

    private final Producer producer;
    private final ApacheSpark spark;

    @PostMapping("/publish")
    public void produce() throws IOException, URISyntaxException {
        final CsvMapper mapper = new CsvMapper();
        final CsvSchema schema = mapper.schemaFor(Cryptocurrency.class).withHeader();
        File fileBitcoin = getFileResource("coin_Bitcoin.csv");
        File fileDogecoin = getFileResource("coin_Dogecoin.csv");

        MappingIterator<Cryptocurrency> bitcoin = mapper
                .readerFor(Cryptocurrency.class)
                .with(schema)
                .readValues(fileBitcoin);

        MappingIterator<Cryptocurrency> dogecoin = mapper
                .readerFor(Cryptocurrency.class)
                .with(schema)
                .readValues(fileDogecoin);

        val cryptocurrencies = new LinkedList<Cryptocurrency>(bitcoin.readAll());
        cryptocurrencies.addAll(dogecoin.readAll());

        cryptocurrencies.forEach(producer::sendMsg);
    }

    @PostMapping("/spark")
    public void spark() {
        spark.proceedData();
    }

    private File getFileResource(final String filename) throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(filename);
        if (resource == null) {
            throw new IllegalArgumentException("file not found! " + filename);
        } else {
            return new File(resource.toURI());
        }
    }
}