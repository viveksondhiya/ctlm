package com.ctlm.statemachine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LcmStateMachineServiceTest {

    private LcmStateMachineService stateMachineService;

    @BeforeEach
    void setUp() {
        stateMachineService = new LcmStateMachineService();
    }

    @Test
    void testValidTransition_UninstantiatedToInstantiating() {
        String cnfId = "cnf-001";
        assertEquals(LcmState.UNINSTANTIATED, stateMachineService.getCurrentState(cnfId));

        LcmState newState = stateMachineService.applyEvent(cnfId, LcmEvent.INSTANTIATE);

        assertEquals(LcmState.INSTANTIATING, newState);
        assertEquals(LcmState.INSTANTIATING, stateMachineService.getCurrentState(cnfId));
    }

    @Test
    void testValidSequence_FullInstantiateAndScaleFlow() {
        String cnfId = "cnf-002";

        stateMachineService.applyEvent(cnfId, LcmEvent.INSTANTIATE);
        assertEquals(LcmState.INSTANTIATING, stateMachineService.getCurrentState(cnfId));

        stateMachineService.applyEvent(cnfId, LcmEvent.INSTANTIATION_COMPLETE);
        assertEquals(LcmState.RUNNING, stateMachineService.getCurrentState(cnfId));

        stateMachineService.applyEvent(cnfId, LcmEvent.SCALE);
        assertEquals(LcmState.SCALING, stateMachineService.getCurrentState(cnfId));
    }

    @Test
    void testInvalidTransition_ThrowsIllegalStateException() {
        String cnfId = "cnf-003";

        assertThrows(IllegalStateException.class, () ->
            stateMachineService.applyEvent(cnfId, LcmEvent.SCALE_COMPLETE)
        );
    }

    @Test
    void testFailTransition_InstantiatingToFailed() {
        String cnfId = "cnf-004";

        stateMachineService.applyEvent(cnfId, LcmEvent.INSTANTIATE);
        assertEquals(LcmState.INSTANTIATING, stateMachineService.getCurrentState(cnfId));

        LcmState failedState = stateMachineService.applyEvent(cnfId, LcmEvent.FAIL);
        assertEquals(LcmState.FAILED, failedState);
        assertEquals(LcmState.FAILED, stateMachineService.getCurrentState(cnfId));
    }

    @Test
    void testIsValidTransition_ReturnsTrueForValid() {
        assertTrue(stateMachineService.isValidTransition(LcmState.UNINSTANTIATED, LcmEvent.INSTANTIATE));
        assertTrue(stateMachineService.isValidTransition(LcmState.INSTANTIATING, LcmEvent.INSTANTIATION_COMPLETE));
        assertTrue(stateMachineService.isValidTransition(LcmState.RUNNING, LcmEvent.SCALE));
        assertTrue(stateMachineService.isValidTransition(LcmState.RUNNING, LcmEvent.TERMINATE));
        assertTrue(stateMachineService.isValidTransition(LcmState.SCALING, LcmEvent.SCALE_COMPLETE));
        assertTrue(stateMachineService.isValidTransition(LcmState.TERMINATING, LcmEvent.TERMINATION_COMPLETE));
    }

    @Test
    void testIsValidTransition_ReturnsFalseForInvalid() {
        assertFalse(stateMachineService.isValidTransition(LcmState.UNINSTANTIATED, LcmEvent.SCALE_COMPLETE));
        assertFalse(stateMachineService.isValidTransition(LcmState.RUNNING, LcmEvent.INSTANTIATE));
        assertFalse(stateMachineService.isValidTransition(LcmState.TERMINATED, LcmEvent.SCALE));
        assertFalse(stateMachineService.isValidTransition(LcmState.FAILED, LcmEvent.TERMINATE));
    }

    @Test
    void testTerminateFlow() {
        String cnfId = "cnf-005";

        stateMachineService.applyEvent(cnfId, LcmEvent.INSTANTIATE);
        stateMachineService.applyEvent(cnfId, LcmEvent.INSTANTIATION_COMPLETE);
        stateMachineService.applyEvent(cnfId, LcmEvent.TERMINATE);
        assertEquals(LcmState.TERMINATING, stateMachineService.getCurrentState(cnfId));

        LcmState terminated = stateMachineService.applyEvent(cnfId, LcmEvent.TERMINATION_COMPLETE);
        assertEquals(LcmState.TERMINATED, terminated);
    }
}
