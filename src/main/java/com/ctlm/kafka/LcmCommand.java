package com.ctlm.kafka;

import com.ctlm.statemachine.LcmEvent;

public class LcmCommand {

    private String cnfId;
    private String cnfName;
    private LcmEvent event;
    private String message;

    public LcmCommand() {}

    public LcmCommand(String cnfId, String cnfName, LcmEvent event, String message) {
        this.cnfId = cnfId;
        this.cnfName = cnfName;
        this.event = event;
        this.message = message;
    }

    public String getCnfId() {
        return cnfId;
    }

    public void setCnfId(String cnfId) {
        this.cnfId = cnfId;
    }

    public String getCnfName() {
        return cnfName;
    }

    public void setCnfName(String cnfName) {
        this.cnfName = cnfName;
    }

    public LcmEvent getEvent() {
        return event;
    }

    public void setEvent(LcmEvent event) {
        this.event = event;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
