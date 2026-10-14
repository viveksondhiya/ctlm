package com.ctlm.assistant.dto;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;


public class AssistantRequest {
    private String question;

    public String getQuestion() { return question; }
    public void setQuestion(String question) { this.question = question; }
}
