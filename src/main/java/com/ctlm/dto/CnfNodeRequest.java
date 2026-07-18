package com.ctlm.dto;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CnfNodeRequest {

    private String cnfId;
    private String name;
    private String type;

}
