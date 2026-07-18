package com.ctlm.dto;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@NoArgsConstructor
@AllArgsConstructor
public class LcmResponse {

    private String cnfId;
    private String currentState;
    private String message;
    private LocalDateTime timestamp;
}
