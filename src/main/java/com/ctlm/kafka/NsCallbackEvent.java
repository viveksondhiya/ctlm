package com.ctlm.kafka;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Kafka message published to "lcm-ns-callback" topic.
 * Site Manager consumes this to know the NS instantiation result.
 *
 * status values:
 *   INSTANTIATION_IN_PROGRESS  — trigger received, CNFs being instantiated
 *   INSTANTIATED               — all CNFs reached RUNNING
 *   INSTANTIATION_FAILED       — at least one CNF FAILED
 */

public class NsCallbackEvent {

    private String nsId;
    private String nsName;
    private String nsType;

    /**
     * INSTANTIATED | INSTANTIATION_FAILED
     */
    private String status;

    private String message;
    private LocalDateTime timestamp;

    /**
     * cnfId → state (INSTANTIATING | RUNNING | FAILED) for each child CNF.
     * Lets Site Manager see which CNF succeeded/failed.
     */
    private Map<String, String> cnfStatuses;

    public NsCallbackEvent() {}

    public NsCallbackEvent(String nsId, String nsName, String nsType,
                           String status, String message,
                           LocalDateTime timestamp, Map<String, String> cnfStatuses) {
        this.nsId = nsId;
        this.nsName = nsName;
        this.nsType = nsType;
        this.status = status;
        this.message = message;
        this.timestamp = timestamp;
        this.cnfStatuses = cnfStatuses;
    }

    public String getNsId() { return nsId; }
    public void setNsId(String nsId) { this.nsId = nsId; }

    public String getNsName() { return nsName; }
    public void setNsName(String nsName) { this.nsName = nsName; }

    public String getNsType() { return nsType; }
    public void setNsType(String nsType) { this.nsType = nsType; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }

    public Map<String, String> getCnfStatuses() { return cnfStatuses; }
    public void setCnfStatuses(Map<String, String> cnfStatuses) { this.cnfStatuses = cnfStatuses; }
}
