package com.ctlm.kafka;

import com.ctlm.dto.NsTriggerRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Central Kafka producer.
 *
 * Topics:
 *   lcm-commands          → individual CNF LCM commands (consumed by LcmCommandConsumer)
 *   lcm-ns-create-trigger → NS create trigger from Site Manager (consumed by NsCreateConsumer)
 *   lcm-ns-callback       → NS instantiation result back to Site Manager
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private static final String TOPIC_LCM_COMMANDS    = "lcm-commands";
    private static final String TOPIC_NS_TRIGGER      = "lcm-ns-create-trigger";
    private static final String TOPIC_NS_CALLBACK     = "lcm-ns-callback";

    // KafkaTemplate<String, Object> works with JsonSerializer for any payload type
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Publish an individual CNF LCM command.
     * Used by: LcmController (scale/terminate/etc.) and NsCreateConsumer (INSTANTIATE).
     */
    public void publishCommand(LcmCommand command) {
        log.info("Publishing LCM command: cnfId={}, event={}", command.getCnfId(), command.getEvent());
        kafkaTemplate.send(TOPIC_LCM_COMMANDS, command.getCnfId(), command);
    }

    /**
     * Publish an NS create trigger from Site Manager's REST call.
     * Consumed by: NsCreateConsumer.
     */
    public void publishNsCreateTrigger(NsTriggerRequest request) {
        log.info("Publishing NS create trigger: nsId={}, nsType={}", request.getNsId(), request.getNsType());
        kafkaTemplate.send(TOPIC_NS_TRIGGER, request.getNsId(), request);
    }

    /**
     * Publish NS instantiation callback to Site Manager.
     * Consumed by: Site Manager's Kafka consumer on "lcm-ns-callback" topic.
     */
    public void publishNsCallback(NsCallbackEvent event) {
        log.info("Publishing NS callback: nsId={}, status={}", event.getNsId(), event.getStatus());
        kafkaTemplate.send(TOPIC_NS_CALLBACK, event.getNsId(), event);
    }
}
