package com.ctlm;

import com.ctlm.statemachine.LcmEvent;
import com.ctlm.statemachine.LcmState;
import com.ctlm.statemachine.LcmStateMachineService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CtlmApplicationTests {

    @Test
    void stateMachineInitialStateIsUninstantiated() {
        LcmStateMachineService service = new LcmStateMachineService();
        assertEquals(LcmState.UNINSTANTIATED, service.getCurrentState("any-cnf"));
    }

    @Test
    void stateMachineAppliesValidTransition() {
        LcmStateMachineService service = new LcmStateMachineService();
        LcmState result = service.applyEvent("cnf-test", LcmEvent.INSTANTIATE);
        assertEquals(LcmState.INSTANTIATION_IN_PROGRESS, result);
    }

    @Test
    void stateMachineRejectsInvalidTransition() {
        LcmStateMachineService service = new LcmStateMachineService();
        assertThrows(IllegalStateException.class, () ->
            service.applyEvent("cnf-test", LcmEvent.TERMINATION_COMPLETE)
        );
    }
}
