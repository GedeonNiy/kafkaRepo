package com.kafkaListener.listener;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;

@Service
@Log4j2
public class KafkaListener implements AcknowledgingConsumerAwareMessageListener<String, JsonNode> {

    @Autowired
    KafkaUtils kafkaUtils;

    @PostConstruct
    void postConstruct(){
        ContainerProperties containerProperties = new ContainerProperties("my-topic");
        final Map<String, Object> consumerProperties = kafkaUtils.kafkaConsumerProperties();
        KafkaMessageListenerContainer<String, JsonNode> container =
                new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(consumerProperties), containerProperties);
    container.setupMessageListener(this);
    container.start();
    //log.info("container for my kafka project is "+container);
    log.info("kafka is listening to consumerProperties" + consumerProperties);
    }


    @Override
    public void onMessage(ConsumerRecord<String, JsonNode> message, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        log.info("message received = {}", message);
        final JsonNode jsonNode = message.value();
        log.info("json message is " + jsonNode.toString());
        log.info("acknowledment" + acknowledgment);
    }
}
