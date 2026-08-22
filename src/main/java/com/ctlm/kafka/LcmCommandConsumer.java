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

        // Sync from Neo4j ONLY if current in-memory state cannot handle this event.
        // This avoids overwriting UNINSTANTIATED with INSTANTIATION_IN_PROGRESS
        // when topology phase pre-sets Neo4j state before state machine runs.
        if (!stateMachineService.isValidTransition(currentState, command.getEvent())) {
            topologyService.getCnf(cnfId).ifPresent(cnf -> {
                try {
                    LcmState neo4jState = LcmState.valueOf(cnf.getCurrentState());
                    stateMachineService.forceState(cnfId, neo4jState);
                    log.info("Restored state from Neo4j: cnfId={}, state={}", cnfId, neo4jState);
                } catch (IllegalArgumentException e) {
                    log.warn("Unknown state in Neo4j for cnfId={}: {}", cnfId, cnf.getCurrentState());
                }
            });
            currentState = stateMachineService.getCurrentState(cnfId); // re-read after sync
        }

        try {
            LcmState newState = stateMachineService.applyEvent(cnfId, command.getEvent());
            topologyService.updateState(cnfId, newState.name());
            auditService.log(cnfId, currentState, newState, command.getEvent(), command.getMessage());
            log.info("State transition: cnfId={} | {} → {} | event={}",
                    cnfId, currentState, newState, command.getEvent());
            nsStateService.onCnfStateChange(cnfId, newState);

        } catch (IllegalStateException e) {
            log.error("Invalid state transition: cnfId={}, state={}, event={} — {}",
                    cnfId, currentState, command.getEvent(), e.getMessage());
            auditService.log(cnfId, currentState, null, command.getEvent(),
                    "FAILED: " + e.getMessage());
        }

//        // Sync state from Neo4j if in-memory is stale (handles app restart case)
//        LcmState currentState = stateMachineService.getCurrentState(cnfId);
//        if (currentState == LcmState.UNINSTANTIATED) {
//            topologyService.getCnf(cnfId).ifPresent(cnf -> {
//                try {
//                    LcmState neo4jState = LcmState.valueOf(cnf.getCurrentState());
//                    if (neo4jState != LcmState.UNINSTANTIATED) {
//                        stateMachineService.forceState(cnfId, neo4jState);
//                        log.info("Restored state from Neo4j: cnfId={}, state={}", cnfId, neo4jState);
//                    }
//                } catch (IllegalArgumentException e) {
//                    log.warn("Unknown state in Neo4j for cnfId={}: {}", cnfId, cnf.getCurrentState());
//                }
//            });
//        }
//
//        currentState = stateMachineService.getCurrentState(cnfId); // re-read after sync
//
//        try {
//            // Apply state machine transition
//            LcmState newState = stateMachineService.applyEvent(cnfId, command.getEvent());
//
//            // Persist new state in Neo4j
//            topologyService.updateState(cnfId, newState.name());
//
//            // Audit trail
//            auditService.log(cnfId, currentState, newState, command.getEvent(), command.getMessage());
//
//            log.info("State transition: cnfId={} | {} → {} | event={}", cnfId, currentState, newState, command.getEvent());
//
//            // Notify NS-level state tracker
//            // If this CNF is a child of an NS, NsStateService will check if
//            // the NS has reached INSTANTIATED or INSTANTIATION_FAILED
//            nsStateService.onCnfStateChange(cnfId, newState);
//
//        } catch (IllegalStateException e) {
//            log.error("Invalid state transition: cnfId={}, state={}, event={} — {}",
//                cnfId, currentState, command.getEvent(), e.getMessage());
//            auditService.log(cnfId, currentState, null, command.getEvent(),
//                "FAILED: " + e.getMessage());
//        }
    }
}
