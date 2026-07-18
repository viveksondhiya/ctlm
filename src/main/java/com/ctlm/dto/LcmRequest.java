package com.ctlm.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class LcmRequest {

    private String cnfId;
    private String cnfName;
    private String message;

}
