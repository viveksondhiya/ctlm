package com.ctlm.statemachine;

public enum LcmState {
    UNINSTANTIATED,
    INSTANTIATING,
    RUNNING,
    SCALING,
    TERMINATING,
    TERMINATED,
    FAILED
}
