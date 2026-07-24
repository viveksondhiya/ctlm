package com.ctlm.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * Payload sent when IPTX / ZTP triggers an NS instantiation.
 * Matches the entityCharacteristic list format from the inventory API.
 *
 * Example:
 * {
 *   "nsId"   : "e77e8372-9376-4d14-a9f0-0db9de201d69",
 *   "name"   : "uhn1tky817500083",
 *   "nsType" : "GnodeB Sub6",
 *   "characteristics": [
 *     {"name": "status",      "value": "INSTANTIATION_FAILED"},
 *     {"name": "technology",  "value": "T5G"},
 *     {"name": "vendor",      "value": "ALTIOSTAR"}
 *   ]
 * }
 *
 * nsType drives which CNFs are auto-created:
 *   "GnodeB Sub6"  → MACRO_SUB6_CU_CP + MACRO_SUB6_CU_UP + MACRO_SUB6_DU
 *   "GnodeB NGDU"  → NGDU (single node, different hierarchy)
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class NsTriggerRequest {

    private String nsId;
    private String name;

    /**
     * nsType from the NS entityCharacteristic, e.g.:
     *   "GnodeB Sub6"  — MacroSub6 gNB
     *   "GnodeB NGDU"  — Next-Gen DU
     */
    private String nsType;

    /** entityCharacteristic list, name-value pairs */
    private List<CharacteristicEntry> characteristics;

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CharacteristicEntry {
        private String name;
        private String value;
    }
}
