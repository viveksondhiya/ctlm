package com.ctlm.topology;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class TopologyService {

    private final CnfNodeRepository cnfNodeRepository;

    public CnfNode registerCnf(String cnfId, String name, String type) {
        CnfNode node = new CnfNode(cnfId, name, type, "UNINSTANTIATED");
        log.info("Registering CNF: id={}, name={}, type={}", cnfId, name, type);
        return cnfNodeRepository.save(node);
    }

    public void updateState(String cnfId, String newState) {
        cnfNodeRepository.findByCnfId(cnfId).ifPresent(node -> {
            node.setCurrentState(newState);
            cnfNodeRepository.save(node);
            log.info("Updated state for CNF {}: {}", cnfId, newState);
        });
    }

    public Optional<CnfNode> getCnf(String cnfId) {
        return cnfNodeRepository.findByCnfId(cnfId);
    }

    public void addDependency(String cnfId, String dependsOnCnfId) {
        CnfNode node = cnfNodeRepository.findByCnfId(cnfId)
            .orElseThrow(() -> new IllegalArgumentException("CNF not found: " + cnfId));
        CnfNode dependency = cnfNodeRepository.findByCnfId(dependsOnCnfId)
            .orElseThrow(() -> new IllegalArgumentException("Dependency CNF not found: " + dependsOnCnfId));

        List<CnfNode> deps = node.getDependencies() != null ? new ArrayList<>(node.getDependencies()) : new ArrayList<>();
        deps.add(dependency);
        node.setDependencies(deps);
        cnfNodeRepository.save(node);
        log.info("Added dependency: {} -> {}", cnfId, dependsOnCnfId);
    }

    public List<CnfNode> getDependencies(String cnfId) {
        return cnfNodeRepository.findByCnfId(cnfId)
            .map(node -> node.getDependencies() != null ? node.getDependencies() : new ArrayList<CnfNode>())
            .orElse(new ArrayList<>());
    }
}
