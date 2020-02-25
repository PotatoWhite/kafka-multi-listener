package me.potato.kafkamultilistener.config;

import lombok.extern.slf4j.Slf4j;
import me.potato.kafkamultilistener.models.Test01;
import me.potato.kafkamultilistener.models.Test02;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaConfig {
    public static final String TOPIC01 = "testTopic01";
    public static final String TOPIC02 = "testTopic02";

    @Bean
    public NewTopic createTestTopic01() {
        return new NewTopic(TOPIC01, 1, (short) 1);
    }

    @Bean
    public NewTopic createTestTopic02() {
        return new NewTopic(TOPIC02, 1, (short) 1);
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory consumerFactory,
            KafkaTemplate kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setErrorHandler(new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), new FixedBackOff(0L, 2)));
        return factory;
    }

    @Bean
    public RecordMessageConverter converter() {

        Map<String, Class<?>> mappings = new HashMap<>();
        mappings.put("Test01", Test01.class);
        mappings.put("Test02", Test02.class);

        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
        typeMapper.setIdClassMapping(mappings);

        StringJsonMessageConverter converter = new StringJsonMessageConverter();
        converter.setTypeMapper(typeMapper);

        return converter;
    }
}
