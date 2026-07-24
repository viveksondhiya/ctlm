package com.ctlm.controller;

import com.ctlm.audit.AuditLog;
import com.ctlm.audit.AuditService;
import com.ctlm.dto.LcmRequest;
import com.ctlm.dto.LcmResponse;
import com.ctlm.dto.NsTriggerRequest;
import com.ctlm.kafka.KafkaProducerService;
import com.ctlm.kafka.LcmCommand;
import com.ctlm.statemachine.LcmEvent;
import com.ctlm.statemachine.LcmStateMachineService;
import com.ctlm.topology.NsStateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/lcm")
@RequiredArgsConstructor
public class LcmController {

    private final KafkaProducerService kafkaProducerService;
    private final LcmStateMachineService stateMachineService;
    private final AuditService auditService;
    private final NsStateService nsStateService;

    @GetMapping("/ping")
    public String ping() {
        return "ping Successfully";
    }

    // ──────────────────────────────────────────────────────────
    //  NS Trigger  (Site Manager → CTLM)
    // ──────────────────────────────────────────────────────────

    /**
     * Site Manager calls this to kick off NS instantiation.
     *
     * What happens:
     *   1. Request published to Kafka "lcm-ns-create-trigger"
     *   2. NsCreateConsumer picks it up → creates NS + 3 CNFs in Neo4j
     *   3. INSTANTIATE command fired for each CNF
     *   4. When all 3 CNFs reach RUNNING → callback published to "lcm-ns-callback"
     *   5. Site Manager consumes callback and sees INSTANTIATED / INSTANTIATION_FAILED
     *
     * POST /api/lcm/ns/trigger
     * Body: { "nsId":"...", "name":"...", "nsType":"GnodeB Sub6",
     *          "characteristics":[{"name":"status","value":"INSTANTIATION_FAILED"},...] }
     */
    @PostMapping("/ns/trigger")
    public ResponseEntity<LcmResponse> triggerNsCreate(@RequestBody NsTriggerRequest request) {
        log.info("NS create trigger received from Site Manager:{}",request);
        kafkaProducerService.publishNsCreateTrigger(request);
        return ResponseEntity.accepted().body(
            new LcmResponse(request.getNsId(), "INSTANTIATION_IN_PROGRESS",
                    "NS create trigger accepted — processing asynchronously", LocalDateTime.now())
        );
    }

    /**
     * Get the current NS instantiation status (all child CNF states).
     * GET /api/lcm/ns/{nsId}/status
     */
    @GetMapping("/ns/{nsId}/status")
    public ResponseEntity<Map<String, String>> getNsStatus(@PathVariable String nsId) {
        Map<String, String> status = nsStateService.getNsInstantiationStatus(nsId);
        if (status.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(status);
    }

    // ──────────────────────────────────────────────────────────
    //  Individual CNF LCM operations
    // ──────────────────────────────────────────────────────────

    @PostMapping("/{cnfId}/instantiate")
    public ResponseEntity<LcmResponse> instantiate(@PathVariable String cnfId, @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Instantiate requested";
        kafkaProducerService.publishCommand(new LcmCommand(cnfId, cnfName, LcmEvent.INSTANTIATE, message));
        return ResponseEntity.accepted().body(
            new LcmResponse(cnfId, "INSTANTIATION_IN_PROGRESS", "Instantiate command accepted", LocalDateTime.now()));
    }

    @PostMapping("/{cnfId}/scale")
    public ResponseEntity<LcmResponse> scale(@PathVariable String cnfId, @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Scale requested";
        kafkaProducerService.publishCommand(new LcmCommand(cnfId, cnfName, LcmEvent.SCALE, message));
        return ResponseEntity.accepted().body(
            new LcmResponse(cnfId, "SCALING", "Scale command accepted", LocalDateTime.now()));
    }

    @PostMapping("/{cnfId}/terminate")
    public ResponseEntity<LcmResponse> terminate(@PathVariable String cnfId,
                                                  @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Terminate requested";
        kafkaProducerService.publishCommand(new LcmCommand(cnfId, cnfName, LcmEvent.TERMINATE, message));
        return ResponseEntity.accepted().body(
            new LcmResponse(cnfId, "TERMINATING", "Terminate command accepted", LocalDateTime.now()));
    }

    /**
     * Called when the underlying CNF platform confirms instantiation is done.
     * In NS-trigger flow, Site Manager does NOT need to call this —
     * it is called internally or by the CNF adapter when the CNF is up.
     *
     * POST /api/lcm/{cnfId}/complete-instantiation
     */
    @PostMapping("/{cnfId}/complete-instantiation")
    public ResponseEntity<LcmResponse> completeInstantiation(@PathVariable String cnfId,
                                                              @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Instantiation completed";
        kafkaProducerService.publishCommand(
            new LcmCommand(cnfId, cnfName, LcmEvent.INSTANTIATION_COMPLETE, message));
        return ResponseEntity.accepted().body(
            new LcmResponse(cnfId, "INSTANTIATED", "Instantiation complete command accepted", LocalDateTime.now()));
    }

    @PostMapping("/{cnfId}/complete-scale")
    public ResponseEntity<LcmResponse> completeScale(@PathVariable String cnfId,
                                                      @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Scale completed";
        kafkaProducerService.publishCommand(
            new LcmCommand(cnfId, cnfName, LcmEvent.SCALE_COMPLETE, message));
        return ResponseEntity.accepted().body(
            new LcmResponse(cnfId, "INSTANTIATED", "Scale complete command accepted", LocalDateTime.now()));
    }

    @PostMapping("/{cnfId}/complete-termination")
    public ResponseEntity<LcmResponse> completeTermination(@PathVariable String cnfId,
                                                            @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Termination completed";
        kafkaProducerService.publishCommand(
            new LcmCommand(cnfId, cnfName, LcmEvent.TERMINATION_COMPLETE, message));
        return ResponseEntity.accepted().body(
            new LcmResponse(cnfId, "TERMINATED", "Termination complete command accepted", LocalDateTime.now()));
    }

    /**
     * Mark a CNF instantiation as failed (called by CNF adapter on error).
     * NsStateService will set NS state to INSTANTIATION_FAILED and send callback.
     *
     * POST /api/lcm/{cnfId}/fail
     */
    @PostMapping("/{cnfId}/fail")
    public ResponseEntity<LcmResponse> fail(@PathVariable String cnfId,
                                             @RequestBody(required = false) LcmRequest request) {
        String cnfName = request != null ? request.getCnfName() : cnfId;
        String message = request != null ? request.getMessage() : "Instantiation failed";
        kafkaProducerService.publishCommand(new LcmCommand(cnfId, cnfName, LcmEvent.FAIL, message));
        return ResponseEntity.accepted().body(
            new LcmResponse(cnfId, "INSTANTIATION_FAILED", "Fail command accepted", LocalDateTime.now()));
    }

    @GetMapping("/{cnfId}/state")
    public ResponseEntity<LcmResponse> getState(@PathVariable String cnfId) {
        String currentState = stateMachineService.getCurrentState(cnfId).name();
        return ResponseEntity.ok(
            new LcmResponse(cnfId, currentState, "Current state retrieved", LocalDateTime.now()));
    }

    @GetMapping("/{cnfId}/history")
    public ResponseEntity<List<AuditLog>> getHistory(@PathVariable String cnfId) {
        return ResponseEntity.ok(auditService.getHistory(cnfId));
    }
}
