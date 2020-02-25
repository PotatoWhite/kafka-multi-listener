package me.potato.kafkamultilistener.controllers;

import lombok.extern.slf4j.Slf4j;
import me.potato.kafkamultilistener.config.KafkaConfig;
import me.potato.kafkamultilistener.models.Test01;
import me.potato.kafkamultilistener.models.Test02;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class TestController {

    public final KafkaTemplate kafkaTemplate;

    public TestController(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/api/testpub01")
    public void publish01(){
        log.info("run test01");
        kafkaTemplate.send(KafkaConfig.TOPIC01, new Test01("testdata01", "testdata02"));
    }

    @GetMapping("/api/testpub02")
    public void publish02(){
        log.info("run test02");
        kafkaTemplate.send(KafkaConfig.TOPIC02, new Test02(100,200));
    }
}
