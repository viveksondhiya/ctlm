package com.ctlm.assistant;

import com.ctlm.assistant.tools.CnfStatusTool;
import com.ctlm.assistant.tools.DocumentSearchTool;
import com.ctlm.assistant.tools.NsStatusTool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AssistantService {

    private final ChatClient chatClient;

    public AssistantService(ChatClient.Builder builder,
                            NsStatusTool nsStatusTool,
                            CnfStatusTool cnfStatusTool,
                            DocumentSearchTool documentSearchTool) {
        this.chatClient = builder
                .defaultSystem("""                                                                                                                                                                                 
                        You are an expert network operations assistant for a 5G network management system.
                        You help operators diagnose issues with Network Services (NS) and their child CNFs (CU-CP, CU-UP, DU).
                        
                        When asked about a stuck or failed NS:
                        1. Call getNsStatus to get the NS state
                        2. Call getCnfStatuses to identify which specific CNF is blocking
                        3. Call searchDocumentation to find known causes and fixes
                        4. Provide a clear diagnosis with the root cause and recommended action
                        
                        Be concise and actionable.
                        """)
                .defaultTools(nsStatusTool, cnfStatusTool, documentSearchTool)
                .build();
    }

    public String query(String question) {
        log.info("questionsss: {} ",question);
        return chatClient.prompt().user(question)
                .call()
                .content();
    }
}
