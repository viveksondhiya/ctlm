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
    void testInitialState_IsUninstantiated() {
        assertEquals(LcmState.UNINSTANTIATED, stateMachineService.getCurrentState("cnf-new"));
    }

    @Test
    void testInstantiate_UninstantiatedToInstantiationInProgress() {
        LcmState next = stateMachineService.applyEvent("cnf-001", LcmEvent.INSTANTIATE);
        assertEquals(LcmState.INSTANTIATION_IN_PROGRESS, next);
        assertEquals(LcmState.INSTANTIATION_IN_PROGRESS, stateMachineService.getCurrentState("cnf-001"));
    }

    @Test
    void testInstantiationComplete_InProgressToInstantiated() {
        stateMachineService.applyEvent("cnf-002", LcmEvent.INSTANTIATE);
        LcmState next = stateMachineService.applyEvent("cnf-002", LcmEvent.INSTANTIATION_COMPLETE);
        assertEquals(LcmState.INSTANTIATED, next);
    }

    @Test
    void testFullFlow_InstantiateScaleTerminate() {
        String id = "cnf-003";

        stateMachineService.applyEvent(id, LcmEvent.INSTANTIATE);
        assertEquals(LcmState.INSTANTIATION_IN_PROGRESS, stateMachineService.getCurrentState(id));

        stateMachineService.applyEvent(id, LcmEvent.INSTANTIATION_COMPLETE);
        assertEquals(LcmState.INSTANTIATED, stateMachineService.getCurrentState(id));

        stateMachineService.applyEvent(id, LcmEvent.SCALE);
        assertEquals(LcmState.SCALING, stateMachineService.getCurrentState(id));

        stateMachineService.applyEvent(id, LcmEvent.SCALE_COMPLETE);
        assertEquals(LcmState.INSTANTIATED, stateMachineService.getCurrentState(id));

        stateMachineService.applyEvent(id, LcmEvent.TERMINATE);
        assertEquals(LcmState.TERMINATING, stateMachineService.getCurrentState(id));

        stateMachineService.applyEvent(id, LcmEvent.TERMINATION_COMPLETE);
        assertEquals(LcmState.TERMINATED, stateMachineService.getCurrentState(id));
    }

    @Test
    void testFail_InProgressToInstantiationFailed() {
        stateMachineService.applyEvent("cnf-004", LcmEvent.INSTANTIATE);
        LcmState failed = stateMachineService.applyEvent("cnf-004", LcmEvent.FAIL);
        assertEquals(LcmState.INSTANTIATION_FAILED, failed);
    }

    @Test
    void testFail_DuringScaling() {
        String id = "cnf-005";
        stateMachineService.applyEvent(id, LcmEvent.INSTANTIATE);
        stateMachineService.applyEvent(id, LcmEvent.INSTANTIATION_COMPLETE);
        stateMachineService.applyEvent(id, LcmEvent.SCALE);
        LcmState failed = stateMachineService.applyEvent(id, LcmEvent.FAIL);
        assertEquals(LcmState.INSTANTIATION_FAILED, failed);
    }

    @Test
    void testInvalidTransition_ThrowsIllegalStateException() {
        // UNINSTANTIATED + SCALE_COMPLETE = invalid
        assertThrows(IllegalStateException.class, () ->
            stateMachineService.applyEvent("cnf-006", LcmEvent.SCALE_COMPLETE)
        );
    }

    @Test
    void testInvalidTransition_CannotInstantiateWhenAlreadyInstantiated() {
        String id = "cnf-007";
        stateMachineService.applyEvent(id, LcmEvent.INSTANTIATE);
        stateMachineService.applyEvent(id, LcmEvent.INSTANTIATION_COMPLETE);
        // INSTANTIATED + INSTANTIATE = invalid
        assertThrows(IllegalStateException.class, () ->
            stateMachineService.applyEvent(id, LcmEvent.INSTANTIATE)
        );
    }

    @Test
    void testIsValidTransition_ValidCases() {
        assertTrue(stateMachineService.isValidTransition(LcmState.UNINSTANTIATED,            LcmEvent.INSTANTIATE));
        assertTrue(stateMachineService.isValidTransition(LcmState.INSTANTIATION_IN_PROGRESS, LcmEvent.INSTANTIATION_COMPLETE));
        assertTrue(stateMachineService.isValidTransition(LcmState.INSTANTIATION_IN_PROGRESS, LcmEvent.FAIL));
        assertTrue(stateMachineService.isValidTransition(LcmState.INSTANTIATED,              LcmEvent.SCALE));
        assertTrue(stateMachineService.isValidTransition(LcmState.INSTANTIATED,              LcmEvent.TERMINATE));
        assertTrue(stateMachineService.isValidTransition(LcmState.SCALING,                   LcmEvent.SCALE_COMPLETE));
        assertTrue(stateMachineService.isValidTransition(LcmState.TERMINATING,               LcmEvent.TERMINATION_COMPLETE));
    }

    @Test
    void testIsValidTransition_InvalidCases() {
        assertFalse(stateMachineService.isValidTransition(LcmState.UNINSTANTIATED,  LcmEvent.SCALE_COMPLETE));
        assertFalse(stateMachineService.isValidTransition(LcmState.INSTANTIATED,    LcmEvent.INSTANTIATE));
        assertFalse(stateMachineService.isValidTransition(LcmState.TERMINATED,      LcmEvent.SCALE));
        assertFalse(stateMachineService.isValidTransition(LcmState.INSTANTIATION_FAILED, LcmEvent.TERMINATE));
    }
}
