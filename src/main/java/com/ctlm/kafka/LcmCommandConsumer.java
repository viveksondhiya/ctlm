package com.ctlm.kafka;

import com.ctlm.audit.AuditService;
import com.ctlm.statemachine.LcmState;
import com.ctlm.statemachine.LcmStateMachineService;
import com.ctlm.topology.TopologyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class LcmCommandConsumer {

    private final LcmStateMachineService stateMachineService;
    private final TopologyService topologyService;
    private final AuditService auditService;

    @KafkaListener(topics = "lcm-commands", groupId = "lcm-orchestrator")
    public void consume(LcmCommand command) {
        String cnfId = command.getCnfId();
        log.info("Consuming command: cnfId={}, event={}", cnfId, command.getEvent());

        LcmState currentState = stateMachineService.getCurrentState(cnfId);

        try {
            LcmState newState = stateMachineService.applyEvent(cnfId, command.getEvent());
            topologyService.updateState(cnfId, newState.name());
            auditService.log(cnfId, currentState, newState, command.getEvent(), command.getMessage());
            log.info("State transition applied: cnfId={} {} -> {}", cnfId, currentState, newState);
        } catch (IllegalStateException e) {
            log.error("Invalid state transition for cnfId={}: {}", cnfId, e.getMessage());
            auditService.log(cnfId, currentState, null, command.getEvent(),
                "FAILED: " + e.getMessage());
        }
    }
}
