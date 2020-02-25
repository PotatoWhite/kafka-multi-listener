package me.potato.kafkamultilistener.listener;

import lombok.extern.slf4j.Slf4j;
import me.potato.kafkamultilistener.config.KafkaConfig;
import me.potato.kafkamultilistener.models.Test01;
import me.potato.kafkamultilistener.models.Test02;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@KafkaListener(id="multiGroup", topics = {KafkaConfig.TOPIC01, KafkaConfig.TOPIC02})
public class TestListener {
    @KafkaHandler
    public void on(Test01 in) {
        log.info("Received(Test01): {} ", in);
    }

    @KafkaHandler
    public void on(Test02 in) {
        log.info("Received(Test02): {} ", in);
    }

    @KafkaHandler(isDefault = true)
    public void unknown(Object object) {
        System.out.println("Received unknown: " + object);
    }
}
