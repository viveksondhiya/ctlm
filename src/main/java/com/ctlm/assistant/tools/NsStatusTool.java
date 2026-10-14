package com.ctlm.assistant.tools;

import com.ctlm.topology.TopologyService;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.stereotype.Component;

@Component
public class NsStatusTool {

    private final TopologyService topologyService;

    public NsStatusTool(TopologyService topologyService) {
        this.topologyService = topologyService;
    }

    @Tool(description = "Get the current LCM state of a Network Service (NS) by its ID. " +
            "Returns states like INSTANTIATED, INSTANTIATION_IN_PROGRESS, INSTANTIATION_FAILED, UNINSTANTIATED.")
    public String getNsStatus(String nsId) {
        return topologyService.getCnf(nsId)
                .map(cnf -> "NS [" + nsId + "] current state: " + cnf.getCurrentState())
                .orElse("NS not found: " + nsId);
    }
}
