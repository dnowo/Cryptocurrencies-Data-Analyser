package pl.rpd.projekt;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import pl.rpd.projekt.model.Sample;
import pl.rpd.projekt.producer.Producer;

@RestController
@RequiredArgsConstructor
public class Controller {

    private final Producer producer;

    @PostMapping("/publish")
    public void produce() {
        producer.sendMsg(Sample.builder()
                .id(18L)
                .name("test")
                .build());
    }
}
