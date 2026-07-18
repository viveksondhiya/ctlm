package com.ctlm.controller;

import com.ctlm.audit.AuditLog;
import com.ctlm.audit.AuditService;
import com.ctlm.dto.LcmRequest;
import com.ctlm.dto.LcmResponse;
import com.ctlm.kafka.KafkaProducerService;
import com.ctlm.kafka.LcmCommand;
import com.ctlm.statemachine.LcmEvent;
import com.ctlm.statemachine.LcmStateMachineService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/lcm")
@RequiredArgsConstructor
public class LcmController {

    private final KafkaProducerService kafkaProducerService;
    private final LcmStateMachineService stateMachineService;
    private final AuditService auditService;

    @PostMapping("/{cnfId}/instantiate")
    public ResponseEntity<LcmResponse> instantiate(@PathVariable String cnfId,@RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Instantiate requested";
        kafkaProducerService.publishCommand(new LcmCommand(cnfId, cnfName, LcmEvent.INSTANTIATE, message));
        return ResponseEntity.accepted().body(new LcmResponse(cnfId, "INSTANTIATING", "Instantiate command accepted", LocalDateTime.now()));
    }

    @PostMapping("/{cnfId}/scale")
    public ResponseEntity<LcmResponse> scale(@PathVariable String cnfId,@RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Scale requested";
        kafkaProducerService.publishCommand(new LcmCommand(cnfId, cnfName, LcmEvent.SCALE, message));
        return ResponseEntity.accepted().body(new LcmResponse(cnfId, "SCALING", "Scale command accepted", LocalDateTime.now()));
    }

    @PostMapping("/{cnfId}/terminate")
    public ResponseEntity<LcmResponse> terminate(@PathVariable String cnfId,
                                                  @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Terminate requested";
        kafkaProducerService.publishCommand(new LcmCommand(cnfId, cnfName, LcmEvent.TERMINATE, message));
        return ResponseEntity.accepted().body(new LcmResponse(cnfId, "TERMINATING", "Terminate command accepted", LocalDateTime.now()));
    }

    @PostMapping("/{cnfId}/complete-instantiation")
    public ResponseEntity<LcmResponse> completeInstantiation(@PathVariable String cnfId,
                                                              @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Instantiation completed";
        kafkaProducerService.publishCommand(new LcmCommand(cnfId, cnfName, LcmEvent.INSTANTIATION_COMPLETE, message));
        return ResponseEntity.accepted().body(new LcmResponse(cnfId, "RUNNING", "Instantiation complete command accepted", LocalDateTime.now()));
    }

    @PostMapping("/{cnfId}/complete-scale")
    public ResponseEntity<LcmResponse> completeScale(@PathVariable String cnfId,
                                                      @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Scale completed";
        kafkaProducerService.publishCommand(new LcmCommand(cnfId, cnfName, LcmEvent.SCALE_COMPLETE, message));
        return ResponseEntity.accepted().body(new LcmResponse(cnfId, "RUNNING", "Scale complete command accepted", LocalDateTime.now()));
    }

    @PostMapping("/{cnfId}/complete-termination")
    public ResponseEntity<LcmResponse> completeTermination(@PathVariable String cnfId,
                                                            @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Termination completed";
        kafkaProducerService.publishCommand(new LcmCommand(cnfId, cnfName, LcmEvent.TERMINATION_COMPLETE, message));
        return ResponseEntity.accepted().body(new LcmResponse(cnfId, "TERMINATED", "Termination complete command accepted", LocalDateTime.now()));
    }

    @GetMapping("/{cnfId}/state")
    public ResponseEntity<LcmResponse> getState(@PathVariable String cnfId) {
        String currentState = stateMachineService.getCurrentState(cnfId).name();
        return ResponseEntity.ok(new LcmResponse(cnfId, currentState, "Current state retrieved", LocalDateTime.now()));
    }

    @GetMapping("/{cnfId}/history")
    public ResponseEntity<List<AuditLog>> getHistory(@PathVariable String cnfId) {
        return ResponseEntity.ok(auditService.getHistory(cnfId));
    }
}
