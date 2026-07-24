package com.ctlm.kafka;

import com.ctlm.audit.AuditService;
import com.ctlm.statemachine.LcmState;
import com.ctlm.statemachine.LcmStateMachineService;
import com.ctlm.topology.NsStateService;
import com.ctlm.topology.TopologyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Processes individual CNF LCM commands from "lcm-commands" topic.
 *
 * After each state transition, notifies NsStateService so it can
 * check if the parent NS has reached a terminal state (INSTANTIATED / FAILED).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LcmCommandConsumer {

    private final LcmStateMachineService stateMachineService;
    private final TopologyService topologyService;
    private final AuditService auditService;
    private final NsStateService nsStateService;

    @KafkaListener(topics = "lcm-commands", groupId = "lcm-orchestrator")
    public void consume(LcmCommand command) {
        String cnfId = command.getCnfId();
        log.info("Consuming LCM command: cnfId={}, event={}", cnfId, command.getEvent());

        LcmState currentState = stateMachineService.getCurrentState(cnfId);

        try {
            // Apply state machine transition
            LcmState newState = stateMachineService.applyEvent(cnfId, command.getEvent());

            // Persist new state in Neo4j
            topologyService.updateState(cnfId, newState.name());

            // Audit trail
            auditService.log(cnfId, currentState, newState, command.getEvent(), command.getMessage());

            log.info("State transition: cnfId={} | {} → {} | event={}", cnfId, currentState, newState, command.getEvent());

            // Notify NS-level state tracker
            // If this CNF is a child of an NS, NsStateService will check if
            // the NS has reached INSTANTIATED or INSTANTIATION_FAILED
            nsStateService.onCnfStateChange(cnfId, newState);

        } catch (IllegalStateException e) {
            log.error("Invalid state transition: cnfId={}, state={}, event={} — {}",
                cnfId, currentState, command.getEvent(), e.getMessage());
            auditService.log(cnfId, currentState, null, command.getEvent(),
                "FAILED: " + e.getMessage());
        }
    }
}
