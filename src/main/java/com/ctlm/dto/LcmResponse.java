package com.ctlm.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LcmResponse {

    private String cnfId;
    private String currentState;
    private String message;
    private LocalDateTime timestamp;
}
