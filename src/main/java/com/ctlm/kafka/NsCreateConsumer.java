package com.ctlm.kafka;

import com.ctlm.dto.NsTriggerRequest;
import com.ctlm.statemachine.LcmEvent;
import com.ctlm.topology.Cnf;
import com.ctlm.topology.NsStateService;
import com.ctlm.topology.TopologyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Kafka consumer for "lcm-ns-create-trigger" topic.
 *
 * Full NS instantiation flow:
 *
 *  TOPOLOGY PHASE  (synchronous, inside this consumer)
 *  ─────────────────────────────────────────────────────
 *  triggerNsCreate() creates everything in Neo4j with state INSTANTIATION_IN_PROGRESS:
 *    [NS created]   → currentState = INSTANTIATION_IN_PROGRESS  (Neo4j)
 *    [CUCP created] → currentState = INSTANTIATION_IN_PROGRESS  (Neo4j)
 *    [CUUP created] → currentState = INSTANTIATION_IN_PROGRESS  (Neo4j)
 *    [DU created]   → currentState = INSTANTIATION_IN_PROGRESS  (Neo4j)
 *    NS_CONTAINS + PARENT_OF relationships set
 *
 *  STATE MACHINE PHASE  (two Kafka messages per CNF, same key = ordered)
 *  ─────────────────────────────────────────────────────────────────────
 *  For each CNF, two commands are published to "lcm-commands":
 *
 *    Message 1 — INSTANTIATE  (initial state machine update)
 *      LcmCommandConsumer:
 *        state machine: UNINSTANTIATED → INSTANTIATION_IN_PROGRESS
 *        Neo4j: already INSTANTIATION_IN_PROGRESS  (idempotent)
 *        MySQL: audit log entry written
 *
 *    Message 2 — INSTANTIATION_COMPLETE  (final state machine update)
 *      LcmCommandConsumer:
 *        state machine: INSTANTIATION_IN_PROGRESS → INSTANTIATED
 *        Neo4j: CNF currentState = INSTANTIATED
 *        MySQL: audit log entry written
 *        NsStateService: all 3 INSTANTIATED?
 *          YES → NS currentState = INSTANTIATED → publish callback
 *
 *  No manual /complete-instantiation calls needed.
 *  Kafka partition ordering (same cnfId key) guarantees message 1 before message 2.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NsCreateConsumer {

    private final TopologyService topologyService;
    private final NsStateService nsStateService;
    private final KafkaProducerService kafkaProducerService;

    @KafkaListener(topics = "lcm-ns-create-trigger", groupId = "lcm-orchestrator")
    public void consume(NsTriggerRequest request) {
        String nsId = request.getNsId();
        log.info("=== NS trigger received: nsId={}, nsType={} ===", nsId, request.getNsType());

        try {
            // ── TOPOLOGY PHASE ───────────────────────────────────────────────
            // Creates NS + all CNFs in Neo4j, both with state INSTANTIATION_IN_PROGRESS.
            // Also sets up NS_CONTAINS and PARENT_OF relationships.
            Cnf ns = topologyService.triggerNsCreate(request);
            log.info("[TOPOLOGY] NS + CNFs created in Neo4j: ns:{}", ns.toString());

            // ── REGISTER FOR STATE TRACKING ──────────────────────────────────
            List<Cnf> cnfs = topologyService.connectIn(nsId);
            nsStateService.registerNsCnfs(nsId, cnfs);
            log.info("[TRACK] Registered {} CNFs under NS {} for state tracking",
                cnfs.size(), nsId);

            // ── STATE MACHINE PHASE ───────────────────────────────────────────
            // Publish two ordered Kafka messages per CNF (same key = same partition = ordered).
            // Message 1: INSTANTIATE        → UNINSTANTIATED → INSTANTIATION_IN_PROGRESS
            // Message 2: INSTANTIATION_COMPLETE → INSTANTIATION_IN_PROGRESS → INSTANTIATED
            for (Cnf cnf : cnfs) {
                String cnfId  = cnf.getCnfId();
                String cnfName = cnf.getName();

                // Message 1 — initial state machine update
                kafkaProducerService.publishCommand(new LcmCommand(
                    cnfId, cnfName, LcmEvent.INSTANTIATE,
                    "NS trigger initial: " + nsId
                ));

                // Message 2 — final state machine update (same key → processed after message 1)
                kafkaProducerService.publishCommand(new LcmCommand(
                    cnfId, cnfName, LcmEvent.INSTANTIATION_COMPLETE,
                    "NS trigger complete: " + nsId
                ));

                log.info("[SM] INSTANTIATE + INSTANTIATION_COMPLETE queued for CNF: {} ({})",
                    cnfId, cnf.getSubType());
            }

            log.info("=== NS trigger done: {} — awaiting state machine processing ===", nsId);

        } catch (Exception e) {
            log.error("NS trigger FAILED for nsId={}: {}", nsId, e.getMessage(), e);
            topologyService.updateState(nsId, "INSTANTIATION_FAILED");
        }
    }
}
