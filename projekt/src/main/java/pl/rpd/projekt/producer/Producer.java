package pl.rpd.projekt.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import pl.rpd.projekt.model.Cryptocurrency;

import static pl.rpd.projekt.Constants.TOPIC;

@Slf4j
@Service
@RequiredArgsConstructor
public class Producer {
    private final KafkaTemplate<String, Cryptocurrency> kafkaTemplate;

    public void sendMsg(Cryptocurrency cryptocurrency) {
        log.info("[Producer] " + cryptocurrency.toString());
        kafkaTemplate.send(TOPIC, cryptocurrency);
    }
}
