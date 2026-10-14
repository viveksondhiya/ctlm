package com.ctlm.assistant.controller;

import com.ctlm.assistant.AssistantService;
import com.ctlm.assistant.dto.AssistantRequest;
import com.ctlm.assistant.dto.AssistantResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/assistant")
public class AssistantController {

    private final AssistantService assistantService;

    public AssistantController(AssistantService assistantService) {
        this.assistantService = assistantService;
    }

    @PostMapping("/query")
    public ResponseEntity<AssistantResponse> query(@RequestBody AssistantRequest request) {
        String answer = assistantService.query(request.getQuestion());
        return ResponseEntity.ok(new AssistantResponse(answer));
    }
}