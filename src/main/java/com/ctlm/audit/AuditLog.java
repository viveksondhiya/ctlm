package com.ctlm.audit;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

@Entity
@Table(name = "lcm_audit_log")
public class AuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String cnfId;

    private String fromState;

    private String toState;

    private String event;

    private String message;

    @CreationTimestamp
    private LocalDateTime timestamp;

    public AuditLog() {}

    public AuditLog(String cnfId, String fromState, String toState, String event, String message) {
        this.cnfId = cnfId;
        this.fromState = fromState;
        this.toState = toState;
        this.event = event;
        this.message = message;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCnfId() {
        return cnfId;
    }

    public void setCnfId(String cnfId) {
        this.cnfId = cnfId;
    }

    public String getFromState() {
        return fromState;
    }

    public void setFromState(String fromState) {
        this.fromState = fromState;
    }

    public String getToState() {
        return toState;
    }

    public void setToState(String toState) {
        this.toState = toState;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
