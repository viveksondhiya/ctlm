package com.ctlm.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * Unified response for GET /api/topology/entity/{id}.
 * Works whether the ID belongs to an NS or a CNF.
 *
 * connectOut → parent of this node
 *   CUUP / DU → CUCP
 *   CUCP      → NS
 *   NS        → (empty — super parent has no parent)
 *
 * connectIn  → children of this node
 *   NS   → [CUCP, CUUP, DU]
 *   CUCP → [CUUP, DU]
 *   CUUP / DU → []
 */
@Getter
@Setter
@NoArgsConstructor
public class EntityResponse {

    private String id;

    /**
     * "CNF" or "NetworkService"
     * Maps to @type in inventory payload.
     */
    private String entityType;

    private String name;

    /**
     * For CNF : MACRO_SUB6_CU_CP | MACRO_SUB6_CU_UP | MACRO_SUB6_DU | NGDU …
     * For NS  : GnodeB Sub6 | GnodeB NGDU …
     */
    private String subType;

    /**
     * CU-CP | CU-UP | DU   (CNF only, from cnfneType characteristic)
     */
    private String neType;

    private String currentState;
    private String technology;
    private String vendor;

    /**
     * Full entityCharacteristic key→value map.
     * All characteristics from the inventory payload are available here.
     */
    private Map<String, String> characteristics;

    /**
     * connect.CNF.out — the parent of this entity.
     * Null if this is the NS (top-level super parent).
     */
    private ConnectRef connectOut;

    /**
     * connect.CNF.in — the children of this entity.
     * Empty list if this is a leaf node (CUUP, DU).
     */
    private List<ConnectRef> connectIn;

    /** Slim reference used inside connectOut / connectIn lists */
    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectRef {
        private String id;
        private String name;
        private String entityType;  // CNF or NetworkService
        private String subType;     // MACRO_SUB6_CU_CP, GnodeB Sub6, etc.
        private String neType;      // CU-CP, CU-UP, DU  (CNF only)
    }
}
