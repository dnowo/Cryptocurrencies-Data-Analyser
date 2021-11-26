package pl.rpd.projekt.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import pl.rpd.projekt.cassandra.CryptoRepository;
import pl.rpd.projekt.model.Cryptocurrency;

import static pl.rpd.projekt.Constants.TOPIC;

@Slf4j
@Service
@RequiredArgsConstructor
public class Consumer {

    private final CryptoRepository cryptoRepository;

    @KafkaListener(topics = TOPIC, groupId = "gid")
    public void receiveMsg(Cryptocurrency object) {
        log.info("[Consumer] " + object.toString());
        cryptoRepository.insert(object);
    }
}
