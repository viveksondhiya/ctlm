package com.ctlm.assistant.tools;
import com.ctlm.topology.Cnf;
import com.ctlm.topology.TopologyService;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class CnfStatusTool {

    private final TopologyService topologyService;

    public CnfStatusTool(TopologyService topologyService) {
        this.topologyService = topologyService;
    }

    @Tool(description = "Get the LCM state of all child CNFs (CU-CP, CU-UP, DU) that belong to a Network Service. " +
            "Use this to identify which specific CNF is blocking NS instantiation.")
    public String getCnfStatuses(String nsId) {
        List<Cnf> children = topologyService.connectIn(nsId);

        if (children.isEmpty()) {
            return "No child CNFs found for NS: " + nsId;
        }

        return children.stream()
                .map(cnf -> (cnf.getNeType() != null ? cnf.getNeType() : cnf.getSubType())
                        + " [" + cnf.getCnfId() + "] → " + cnf.getCurrentState())
                .collect(Collectors.joining("\n"));
    }
}
