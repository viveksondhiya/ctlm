package com.ctlm.statemachine;

import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class LcmStateMachineService {

    private final ConcurrentHashMap<String, LcmState> stateMap = new ConcurrentHashMap<>();

    static final Map<LcmState, Map<LcmEvent, LcmState>> TRANSITIONS;

    static {
        TRANSITIONS = new EnumMap<>(LcmState.class);

        Map<LcmEvent, LcmState> uninstantiatedTransitions = new EnumMap<>(LcmEvent.class);
        uninstantiatedTransitions.put(LcmEvent.INSTANTIATE, LcmState.INSTANTIATING);
        TRANSITIONS.put(LcmState.UNINSTANTIATED, uninstantiatedTransitions);

        Map<LcmEvent, LcmState> instantiatingTransitions = new EnumMap<>(LcmEvent.class);
        instantiatingTransitions.put(LcmEvent.INSTANTIATION_COMPLETE, LcmState.RUNNING);
        instantiatingTransitions.put(LcmEvent.FAIL, LcmState.FAILED);
        TRANSITIONS.put(LcmState.INSTANTIATING, instantiatingTransitions);

        Map<LcmEvent, LcmState> runningTransitions = new EnumMap<>(LcmEvent.class);
        runningTransitions.put(LcmEvent.SCALE, LcmState.SCALING);
        runningTransitions.put(LcmEvent.TERMINATE, LcmState.TERMINATING);
        TRANSITIONS.put(LcmState.RUNNING, runningTransitions);

        Map<LcmEvent, LcmState> scalingTransitions = new EnumMap<>(LcmEvent.class);
        scalingTransitions.put(LcmEvent.SCALE_COMPLETE, LcmState.RUNNING);
        scalingTransitions.put(LcmEvent.FAIL, LcmState.FAILED);
        TRANSITIONS.put(LcmState.SCALING, scalingTransitions);

        Map<LcmEvent, LcmState> terminatingTransitions = new EnumMap<>(LcmEvent.class);
        terminatingTransitions.put(LcmEvent.TERMINATION_COMPLETE, LcmState.TERMINATED);
        terminatingTransitions.put(LcmEvent.FAIL, LcmState.FAILED);
        TRANSITIONS.put(LcmState.TERMINATING, terminatingTransitions);
    }

    public LcmState getCurrentState(String cnfId) {
        return stateMap.getOrDefault(cnfId, LcmState.UNINSTANTIATED);
    }

    public LcmState applyEvent(String cnfId, LcmEvent event) {
        LcmState current = getCurrentState(cnfId);
        if (!isValidTransition(current, event)) {
            throw new IllegalStateException(
                String.format("Invalid transition for CNF '%s': state=%s, event=%s", cnfId, current, event)
            );
        }
        LcmState newState = TRANSITIONS.get(current).get(event);
        stateMap.put(cnfId, newState);
        return newState;
    }

    public boolean isValidTransition(LcmState current, LcmEvent event) {
        Map<LcmEvent, LcmState> eventMap = TRANSITIONS.get(current);
        return eventMap != null && eventMap.containsKey(event);
    }
}
