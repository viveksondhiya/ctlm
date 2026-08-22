package com.ctlm.kafka;

import com.ctlm.statemachine.LcmEvent;
import com.ctlm.topology.Cnf;
import com.ctlm.topology.NsStateService;
import com.ctlm.topology.TopologyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class NsTerminateConsumer {

    private final TopologyService topologyService;
    private final NsStateService nsStateService;
    private final KafkaProducerService kafkaProducerService;

    @KafkaListener(topics = "lcm-ns-terminate-trigger", groupId = "lcm-orchestrator")
    public void consume(String nsId) {
        log.info("=== NS terminate trigger received: nsId={} ===", nsId);

        try {
            // TOPOLOGY PHASE
            topologyService.updateState(nsId, "TERMINATING");
            log.info("[TOPOLOGY] NS {} → TERMINATING", nsId);

            // FETCH CHILD CNFs
            List<Cnf> cnfs = topologyService.connectIn(nsId);
            if (cnfs.isEmpty()) {
                log.warn("No child CNFs found for NS {} — skipping termination", nsId);
                return;
            }

            // REGISTER FOR TERMINATION TRACKING
            nsStateService.registerNsTermination(nsId, cnfs);
            log.info("[TRACK] Registered {} CNFs under NS {} for termination tracking",
                    cnfs.size(), nsId);

            // STATE MACHINE PHASE — reverse order: DU → CU-UP → CU-CP
            List<Cnf> reversedCnfs = new ArrayList<>(cnfs);
            Collections.reverse(reversedCnfs);

            for (Cnf cnf : reversedCnfs) {
                String cnfId   = cnf.getCnfId();
                String cnfName = cnf.getName();

                // Message 1 — INSTANTIATED → TERMINATING
                kafkaProducerService.publishCommand(new LcmCommand(
                        cnfId, cnfName, LcmEvent.TERMINATE, "NS terminate trigger: " + nsId + "for cnf :"+cnfId));

                // Message 2 — TERMINATING → TERMINATED (same key = ordered)
                kafkaProducerService.publishCommand(new LcmCommand(
                        cnfId, cnfName, LcmEvent.TERMINATION_COMPLETE, "NS terminate complete: " + nsId  +"for cnf :"+cnfId));

                log.info("[SM] TERMINATE + TERMINATION_COMPLETE queued for CNF: {} ({})", cnfId, cnf.getSubType());
            }

            log.info("=== NS terminate done: {} — awaiting state machine processing ===", nsId);

        } catch (Exception e) {
            log.error("NS terminate FAILED for nsId={}: {}", nsId, e.getMessage(), e);
            topologyService.updateState(nsId, "TERMINATION_FAILED");
        }
    }
}