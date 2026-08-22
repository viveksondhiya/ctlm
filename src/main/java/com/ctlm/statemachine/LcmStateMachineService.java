package com.ctlm.statemachine;

import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory finite state machine for CNF lifecycle.
 *
 * States:
 *   UNINSTANTIATED           → initial state for every new CNF
 *   INSTANTIATION_IN_PROGRESS → INSTANTIATE event fired, CNF being deployed
 *   INSTANTIATED              → deployment complete, CNF is live
 *   SCALING                   → scale operation in progress
 *   TERMINATING               → termination in progress
 *   TERMINATED                → permanently stopped
 *   INSTANTIATION_FAILED      → any FAIL event (instantiation or scale)
 *
 * The state name stored in Neo4j (currentState) exactly matches LcmState.name()
 * so what you see in the DB is directly human-readable.
 */
@Service
public class LcmStateMachineService {

    private final ConcurrentHashMap<String, LcmState> stateMap = new ConcurrentHashMap<>();

    static final Map<LcmState, Map<LcmEvent, LcmState>> TRANSITIONS;

    static {
        TRANSITIONS = new EnumMap<>(LcmState.class);

        // UNINSTANTIATED → INSTANTIATE → INSTANTIATION_IN_PROGRESS
        Map<LcmEvent, LcmState> fromUninstantiated = new EnumMap<>(LcmEvent.class);
        fromUninstantiated.put(LcmEvent.INSTANTIATE, LcmState.INSTANTIATION_IN_PROGRESS);
        TRANSITIONS.put(LcmState.UNINSTANTIATED, fromUninstantiated);

        // INSTANTIATION_IN_PROGRESS → INSTANTIATION_COMPLETE → INSTANTIATED
        //                           → FAIL                   → INSTANTIATION_FAILED
        Map<LcmEvent, LcmState> fromInProgress = new EnumMap<>(LcmEvent.class);
        fromInProgress.put(LcmEvent.INSTANTIATION_COMPLETE, LcmState.INSTANTIATED);
        fromInProgress.put(LcmEvent.FAIL,                   LcmState.INSTANTIATION_FAILED);
        TRANSITIONS.put(LcmState.INSTANTIATION_IN_PROGRESS, fromInProgress);

        // INSTANTIATED → SCALE     → SCALING
        //              → TERMINATE → TERMINATING
        Map<LcmEvent, LcmState> fromInstantiated = new EnumMap<>(LcmEvent.class);
        fromInstantiated.put(LcmEvent.SCALE,     LcmState.SCALING);
        fromInstantiated.put(LcmEvent.TERMINATE, LcmState.TERMINATING);
        TRANSITIONS.put(LcmState.INSTANTIATED, fromInstantiated);

        // SCALING → SCALE_COMPLETE → INSTANTIATED
        //         → FAIL           → INSTANTIATION_FAILED
        Map<LcmEvent, LcmState> fromScaling = new EnumMap<>(LcmEvent.class);
        fromScaling.put(LcmEvent.SCALE_COMPLETE, LcmState.INSTANTIATED);
        fromScaling.put(LcmEvent.FAIL,           LcmState.INSTANTIATION_FAILED);
        TRANSITIONS.put(LcmState.SCALING, fromScaling);

        // TERMINATING → TERMINATION_COMPLETE → TERMINATED
        //             → FAIL                 → INSTANTIATION_FAILED
        Map<LcmEvent, LcmState> fromTerminating = new EnumMap<>(LcmEvent.class);
        fromTerminating.put(LcmEvent.TERMINATION_COMPLETE, LcmState.TERMINATED);
        fromTerminating.put(LcmEvent.FAIL,                 LcmState.INSTANTIATION_FAILED);
        TRANSITIONS.put(LcmState.TERMINATING, fromTerminating);
    }

    public LcmState getCurrentState(String cnfId) {
        return stateMap.getOrDefault(cnfId, LcmState.UNINSTANTIATED);
    }

    public void forceState(String cnfId,LcmState lcmState) {
        stateMap.put(cnfId,lcmState);
    }

    public LcmState applyEvent(String cnfId, LcmEvent event) {
        LcmState current = getCurrentState(cnfId);
        if (!isValidTransition(current, event)) {
            throw new IllegalStateException(String.format(
                "Invalid transition for CNF '%s': state=%s, event=%s", cnfId, current, event));
        }
        LcmState next = TRANSITIONS.get(current).get(event);
        stateMap.put(cnfId, next);
        return next;
    }

    public boolean isValidTransition(LcmState current, LcmEvent event) {
        Map<LcmEvent, LcmState> eventMap = TRANSITIONS.get(current);
        return eventMap != null && eventMap.containsKey(event);
    }
}
