package com.ctlm.controller;

import com.ctlm.dto.CnfNodeRequest;
import com.ctlm.topology.CnfNode;
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

    @PostMapping("/cnf")
    public ResponseEntity<CnfNode> registerCnf(@RequestBody CnfNodeRequest request) {
        CnfNode node = topologyService.registerCnf(request.getCnfId(), request.getName(), request.getType());
        return ResponseEntity.status(HttpStatus.CREATED).body(node);
    }

    @PostMapping("/{cnfId}/dependencies/{dependsOnCnfId}")
    public ResponseEntity<Void> addDependency(@PathVariable String cnfId,
                                               @PathVariable String dependsOnCnfId) {
        topologyService.addDependency(cnfId, dependsOnCnfId);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/{cnfId}")
    public ResponseEntity<CnfNode> getCnf(@PathVariable String cnfId) {
        return topologyService.getCnf(cnfId)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/{cnfId}/dependencies")
    public ResponseEntity<List<CnfNode>> getDependencies(@PathVariable String cnfId) {
        return ResponseEntity.ok(topologyService.getDependencies(cnfId));
    }
}
