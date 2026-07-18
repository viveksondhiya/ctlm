package com.ctlm.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private static final String TOPIC = "lcm-commands";

    private final KafkaTemplate<String, LcmCommand> kafkaTemplate;

    public void publishCommand(LcmCommand command) {
        log.info("Publishing command: cnfId={}, event={}", command.getCnfId(), command.getEvent());
        kafkaTemplate.send(TOPIC, command.getCnfId(), command);
    }
}
