package com.ctlm.statemachine;

public enum LcmState {
    UNINSTANTIATED,
    INSTANTIATION_IN_PROGRESS,
    INSTANTIATED,
    SCALING,
    TERMINATING,
    TERMINATED,
    INSTANTIATION_FAILED
}
