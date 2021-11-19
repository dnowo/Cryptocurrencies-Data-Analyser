package pl.rpd.projekt.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import pl.rpd.projekt.model.Sample;

import static pl.rpd.projekt.Constants.TOPIC;

@Service
@Slf4j
@RequiredArgsConstructor
public class Consumer {
    @KafkaListener(topics = TOPIC, groupId = "gid")
    public void receiveMsg(Sample object) {
        log.info("[Consumer] " + object.toString());
    }
}
