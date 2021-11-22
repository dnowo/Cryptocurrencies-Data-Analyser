package pl.rpd.projekt;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import pl.rpd.projekt.cassandra.CryptoRepository;
import pl.rpd.projekt.model.Cryptocurrency;
import pl.rpd.projekt.model.Sample;
import pl.rpd.projekt.producer.Producer;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

@RestController
@RequiredArgsConstructor
public class Controller {

    private final Producer producer;
    private final CryptoRepository cryptoRepository;

    @PostMapping("/publish")
    public void produce() throws IOException, URISyntaxException {
        producer.sendMsg(Sample.builder()
                .id(18L)
                .name("test")
                .build());

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

        bitcoin.readAll().forEach(cryptoRepository::insert);
        dogecoin.readAll().forEach(cryptoRepository::insert);

//        cryptoRepository.select();

//        cryptoRepository.insert(dto);
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