package com.ctlm.topology;

import com.ctlm.kafka.KafkaProducerService;
import com.ctlm.kafka.NsCallbackEvent;
import com.ctlm.statemachine.LcmState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks NS-level instantiation state based on its child CNF states.
 *
 * When all 3 CNFs (CUCP + CUUP + DU) reach RUNNING:
 *   → NS state = INSTANTIATED
 *   → publish NsCallbackEvent to "lcm-ns-callback" topic (Site Manager receives this)
 *
 * When any CNF reaches FAILED:
 *   → NS state = INSTANTIATION_FAILED
 *   → publish NsCallbackEvent with failure info
 *
 * In-memory tracking is rebuilt on each NS trigger.
 * (For production, persist to DB/Redis for restart resilience.)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NsStateService {

    private final TopologyService topologyService;
    private final KafkaProducerService kafkaProducerService;

    /** nsId → { cnfId → LcmState } */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, LcmState>> nsTracker
        = new ConcurrentHashMap<>();

    /** cnfId → nsId  (reverse lookup) */
    private final ConcurrentHashMap<String, String> cnfToNs = new ConcurrentHashMap<>();

    /** nsIds for which callback has already been sent (prevents duplicates) */
    private final Set<String> callbackSent = ConcurrentHashMap.newKeySet();

    /** nsId → { cnfId → LcmState } — for termination tracking */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, LcmState>> nsTerminationTracker
            = new ConcurrentHashMap<>();

    /** nsIds for which termination callback already sent */
    private final Set<String> terminationCallbackSent = ConcurrentHashMap.newKeySet();

    // ──────────────────────────────────────────────────────────
    //  Registration — called from NsCreateConsumer
    // ──────────────────────────────────────────────────────────

    /**
     * Register all child CNFs for an NS so state changes can be tracked.
     * Called after triggerNsCreate() returns the list of created CNFs.
     */
    public void registerNsCnfs(String nsId, List<Cnf> cnfs) {
        ConcurrentHashMap<String, LcmState> stateMap = new ConcurrentHashMap<>();
        for (Cnf cnf : cnfs) {
            stateMap.put(cnf.getCnfId(), LcmState.UNINSTANTIATED);
            cnfToNs.put(cnf.getCnfId(), nsId);
        }
        nsTracker.put(nsId, stateMap);
        callbackSent.remove(nsId);  // reset in case of re-trigger
        log.info("Registered NS {} with {} CNFs for state tracking", nsId, cnfs.size());
    }

    public void registerNsTermination(String nsId, List<Cnf> cnfs) {
        ConcurrentHashMap<String, LcmState> stateMap = new ConcurrentHashMap<>();
        for (Cnf cnf : cnfs) {
            stateMap.put(cnf.getCnfId(), LcmState.TERMINATING);
            cnfToNs.put(cnf.getCnfId(), nsId);
        }
        nsTerminationTracker.put(nsId, stateMap);
        terminationCallbackSent.remove(nsId);
        log.info("Registered NS {} with {} CNFs for termination tracking", nsId, cnfs.size());
    }

    // ──────────────────────────────────────────────────────────
    //  State change hook — called from LcmCommandConsumer
    // ──────────────────────────────────────────────────────────

    /**
     * Called every time a CNF's LCM state changes.
     * Evaluates whether the parent NS has reached a terminal state.
     *
     * Terminal states:
     *   INSTANTIATED        — all CNFs are RUNNING
     *   INSTANTIATION_FAILED — at least one CNF is FAILED
     */
//    public void onCnfStateChange(String cnfId, LcmState newState) {
//        String nsId = cnfToNs.get(cnfId);
//        if (nsId == null) {
//            log.debug("CNF {} not tracked by any NS — skipping NS state check", cnfId);
//            return;
//        }
//
//        ConcurrentHashMap<String, LcmState> cnfStates = nsTracker.get(nsId);
//        if (cnfStates == null || !cnfStates.containsKey(cnfId)) {
//            log.debug("CNF {} not in NS {} tracker map", cnfId, nsId);
//            return;
//        }
//
//        cnfStates.put(cnfId, newState);
//        log.info("NS {} | CNF {} → {} | all states: {}", nsId, cnfId, newState, cnfStates);
//
//        // skip if callback already sent for this NS
//        if (callbackSent.contains(nsId)) return;
//
//        if (newState == LcmState.INSTANTIATION_FAILED) {
//            // Any failure → NS fails immediately
//            fireCallback(nsId, cnfStates, "INSTANTIATION_FAILED", "Instantiation failed for CNF: " + cnfId);
//        } else if (allInstantiated(cnfStates)) {
//            // All CNFs instantiated successfully
//            fireCallback(nsId, cnfStates, "INSTANTIATED",
//                "All CNFs instantiated successfully");
//        }
//    }

    public void onCnfStateChange(String cnfId, LcmState newState) {
        String nsId = cnfToNs.get(cnfId);
        if (nsId == null) {
            log.debug("CNF {} not tracked by any NS — skipping NS state check", cnfId);
            return;
        }

        // Instantiation tracking
        ConcurrentHashMap<String, LcmState> cnfStates = nsTracker.get(nsId);
        if (cnfStates != null && cnfStates.containsKey(cnfId)) {
            cnfStates.put(cnfId, newState);
            log.info("NS {} | CNF {} → {} | instantiation states: {}", nsId, cnfId, newState, cnfStates);
            if (!callbackSent.contains(nsId)) {
                if (newState == LcmState.INSTANTIATION_FAILED) {
                    fireCallback(nsId, cnfStates, "INSTANTIATION_FAILED",
                            "Instantiation failed for CNF: " + cnfId);
                } else if (allInstantiated(cnfStates)) {
                    fireCallback(nsId, cnfStates, "INSTANTIATED",
                            "All CNFs instantiated successfully");
                }
            }
        }

        // Termination tracking
        ConcurrentHashMap<String, LcmState> termStates = nsTerminationTracker.get(nsId);
        if (termStates != null && termStates.containsKey(cnfId)) {
            termStates.put(cnfId, newState);
            log.info("NS {} | CNF {} → {} | termination states: {}", nsId, cnfId, newState, termStates);
            if (!terminationCallbackSent.contains(nsId)) {
                if (newState == LcmState.INSTANTIATION_FAILED) {
                    fireTerminationCallback(nsId, termStates, "TERMINATION_FAILED",
                            "Termination failed for CNF: " + cnfId);
                } else if (allTerminated(termStates)) {
                    fireTerminationCallback(nsId, termStates, "TERMINATED",
                            "All CNFs terminated successfully");
                }
            }
        }
    }

    // ──────────────────────────────────────────────────────────
    //  NS state query
    // ──────────────────────────────────────────────────────────

    /**
     * Returns a snapshot of all CNF states for the given NS.
     * Used by the status API.
     */
    public Map<String, String> getNsInstantiationStatus(String nsId) {
        ConcurrentHashMap<String, LcmState> states = nsTracker.get(nsId);
        log.info("getNsInstantiationStatus for ns id : {} and hte states is : {} ",nsId,states);
        if (states == null) return Collections.emptyMap();
        Map<String, String> result = new LinkedHashMap<>();
        states.forEach((cnfId, state) -> result.put(cnfId, state.name()));
        return result;
    }

    // ──────────────────────────────────────────────────────────
    //  Private helpers
    // ──────────────────────────────────────────────────────────

    private boolean allInstantiated(Map<String, LcmState> states) {
        return !states.isEmpty()
            && states.values().stream().allMatch(s -> s == LcmState.INSTANTIATED);
    }

    private boolean allTerminated(Map<String, LcmState> states) {
        return !states.isEmpty()
                && states.values().stream().allMatch(s -> s == LcmState.TERMINATED);
    }

    private void fireTerminationCallback(String nsId, Map<String, LcmState> cnfStates,
                                         String status, String message) {
        if (!terminationCallbackSent.add(nsId)) return;

        topologyService.updateState(nsId, status);
        log.info("NS {} state → {}", nsId, status);

        Map<String, String> cnfStatusMap = new LinkedHashMap<>();
        cnfStates.forEach((id, s) -> cnfStatusMap.put(id, s.name()));

        Cnf ns = topologyService.getCnf(nsId).orElse(null);
        String nsName = ns != null ? ns.getName()    : nsId;
        String nsType = ns != null ? ns.getSubType() : "UNKNOWN";

        NsCallbackEvent event = new NsCallbackEvent(
                nsId, nsName, nsType,
                status, message,
                LocalDateTime.now(),
                cnfStatusMap
        );

        kafkaProducerService.publishNsCallback(event);
        log.info("NS termination callback published: nsId={}, status={}", nsId, status);
    }

    private void fireCallback(String nsId, Map<String, LcmState> cnfStates, String status, String message) {
        // Atomic: only one thread fires the callback
        if (!callbackSent.add(nsId)) return;

        // Update NS state in Neo4j
        topologyService.updateState(nsId, status);
        log.info("NS {} state → {}", nsId, status);

        // Build cnfStatuses map for the callback
        Map<String, String> cnfStatusMap = new LinkedHashMap<>();
        cnfStates.forEach((id, s) -> cnfStatusMap.put(id, s.name()));

        // Fetch NS name and type
        Cnf ns = topologyService.getCnf(nsId).orElse(null);
        String nsName = ns != null ? ns.getName()    : nsId;
        String nsType = ns != null ? ns.getSubType() : "UNKNOWN";

        NsCallbackEvent event = new NsCallbackEvent(nsId, nsName, nsType, status, message, LocalDateTime.now(), cnfStatusMap);

        kafkaProducerService.publishNsCallback(event);
        log.info("NS callback published: nsId={}, status={}", nsId, status);
    }
}
