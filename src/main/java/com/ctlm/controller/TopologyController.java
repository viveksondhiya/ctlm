package com.ctlm.controller;

import com.ctlm.dto.CnfNodeRequest;
import com.ctlm.dto.EntityResponse;
import com.ctlm.dto.NsRequest;
import com.ctlm.dto.NsTriggerRequest;
import com.ctlm.topology.Cnf;
import com.ctlm.topology.TopologyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/topology")
@RequiredArgsConstructor
public class TopologyController {

    private final TopologyService topologyService;

    // ──────────────────────────────────────────────────────────
    //  NS Trigger  (IPTX / ZTP → auto-creates NS + child CNFs)
    // ──────────────────────────────────────────────────────────

    /**
     * Triggered by IPTX / ZTP when an NS instantiation starts.
     * Automatically creates:
     *   - NS node  (entityType=NetworkService)
     *   - CNF nodes based on nsType:
     *       GnodeB Sub6  → CUCP (MACRO_SUB6_CU_CP) + CUUP + DU
     *       GnodeB NGDU  → NGDU
     *   - NS_CONTAINS  → NS links to all CNFs
     *   - PARENT_OF    → CUCP links to CUUP + DU
     *
     * POST /api/topology/ns/trigger
     * Body:
     * {
     *   "nsId"  : "e77e8372-...",
     *   "name"  : "uhn1tky817500083",
     *   "nsType": "GnodeB Sub6",
     *   "characteristics": [
     *     {"name":"status",     "value":"INSTANTIATION_PENDING"},
     *     {"name":"technology", "value":"T5G"},
     *     {"name":"vendor",     "value":"ALTIOSTAR"}
     *   ]
     * }
     */
    @PostMapping("/ns/trigger")
    public ResponseEntity<Cnf> triggerNsCreate(@RequestBody NsTriggerRequest request) {
        Cnf ns = topologyService.triggerNsCreate(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(ns);
    }

    // ──────────────────────────────────────────────────────────
    //  Unified entity lookup — works for NS ID or CNF ID
    // ──────────────────────────────────────────────────────────

    /**
     * Returns full entity data regardless of whether the ID is an NS or a CNF.
     * Response includes:
     *   - entityType  : "CNF" or "NetworkService"
     *   - subType     : MACRO_SUB6_CU_CP | GnodeB Sub6 | …
     *   - neType      : CU-CP | CU-UP | DU  (CNF only)
     *   - characteristics : full key→value map
     *   - connectOut  : parent node reference  (null for NS)
     *   - connectIn   : child node references  (empty for leaf CNFs)
     *
     * GET /api/topology/entity/{id}
     */
    @GetMapping("/entity/{id}")
    public ResponseEntity<EntityResponse> getEntity(@PathVariable String id) {
        try {
            return ResponseEntity.ok(topologyService.getEntityById(id));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        }
    }

    // ──────────────────────────────────────────────────────────
    //  connect.CNF.out  /  connect.CNF.in
    // ──────────────────────────────────────────────────────────

    /**
     * connect.CNF.out — returns the parent of this entity.
     *   CUUP / DU → CUCP
     *   CUCP      → NS
     *   NS        → 404
     *
     * GET /api/topology/{cnfId}/connect/out
     */
    @GetMapping("/{cnfId}/connect/out")
    public ResponseEntity<Cnf> connectOut(@PathVariable String cnfId) {
        return topologyService.connectOut(cnfId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /**
     * connect.CNF.in — returns the children of this entity.
     *   NS   → [CUCP, CUUP, DU]
     *   CUCP → [CUUP, DU]
     *   CUUP / DU → []
     *
     * GET /api/topology/{cnfId}/connect/in
     */
    @GetMapping("/{cnfId}/connect/in")
    public ResponseEntity<List<Cnf>> connectIn(@PathVariable String cnfId) {
        return ResponseEntity.ok(topologyService.connectIn(cnfId));
    }

    // ──────────────────────────────────────────────────────────
    //  NS management (manual registration)
    // ──────────────────────────────────────────────────────────

    /** POST /api/topology/ns  — manually register an NS */
    @PostMapping("/ns")
    public ResponseEntity<Cnf> registerNs(@RequestBody NsRequest request) {
        Cnf ns = topologyService.registerNs(request.getNsId(), request.getName());
        return ResponseEntity.status(HttpStatus.CREATED).body(ns);
    }

    /** GET /api/topology/ns  — list all NS nodes */
    @GetMapping("/ns")
    public ResponseEntity<List<Cnf>> getAllNs() {
        return ResponseEntity.ok(topologyService.getAllNs());
    }

    /** POST /api/topology/{nsId}/cnf/{cnfId}  — link CNF under NS manually */
    @PostMapping("/{nsId}/cnf/{cnfId}")
    public ResponseEntity<Void> addNsCnf(@PathVariable String nsId,
                                          @PathVariable String cnfId) {
        topologyService.addNsCnf(nsId, cnfId);
        return ResponseEntity.ok().build();
    }

    /** POST /api/topology/{parentCnfId}/child/{childCnfId}  — set PARENT_OF manually */
    @PostMapping("/{parentCnfId}/child/{childCnfId}")
    public ResponseEntity<Void> addChild(@PathVariable String parentCnfId,
                                          @PathVariable String childCnfId) {
        topologyService.addChild(parentCnfId, childCnfId);
        return ResponseEntity.ok().build();
    }

    // ──────────────────────────────────────────────────────────
    //  CNF management
    // ──────────────────────────────────────────────────────────

    /** POST /api/topology/cnf  — manually register a CNF */
    @PostMapping("/cnf")
    public ResponseEntity<Cnf> registerCnf(@RequestBody CnfNodeRequest request) {
        Cnf node = topologyService.registerCnf(request.getCnfId(), request.getName(), request.getType());
        return ResponseEntity.status(HttpStatus.CREATED).body(node);
    }

    /** GET /api/topology/{cnfId}  — get a CNF/NS node (raw, no connect info) */
    @GetMapping("/{cnfId}")
    public ResponseEntity<Cnf> getCnf(@PathVariable String cnfId) {
        return topologyService.getCnf(cnfId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    /** POST /api/topology/{cnfId}/dependencies/{dependsOnCnfId}  — legacy DEPENDS_ON */
    @PostMapping("/{cnfId}/dependencies/{dependsOnCnfId}")
    public ResponseEntity<Void> addDependency(@PathVariable String cnfId,
                                               @PathVariable String dependsOnCnfId) {
        topologyService.addDependency(cnfId, dependsOnCnfId);
        return ResponseEntity.ok().build();
    }

    /** GET /api/topology/{cnfId}/dependencies  — legacy DEPENDS_ON list */
    @GetMapping("/{cnfId}/dependencies")
    public ResponseEntity<List<Cnf>> getDependencies(@PathVariable String cnfId) {
        return ResponseEntity.ok(topologyService.getDependencies(cnfId));
    }
}
