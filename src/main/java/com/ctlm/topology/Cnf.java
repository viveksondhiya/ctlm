package com.ctlm.topology;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Relationship;

import java.util.List;

/**
 * Unified node for both NS (NetworkService) and CNF entities.
 *
 * entityType   : "CNF" or "NetworkService"  (from @type in inventory JSON)
 * subType      : "MACRO_SUB6_CU_CP" | "MACRO_SUB6_CU_UP" | "MACRO_SUB6_DU"
 *                | "NGDU" | "GnodeB Sub6" (for NS) etc.
 * neType       : "CU-CP" | "CU-UP" | "DU"  (cnfneType characteristic)
 *
 * Hierarchy:
 *   NS  (entityType=NetworkService)  — super parent
 *   ├── CUCP  --[NS_CONTAINS]──  (subType=MACRO_SUB6_CU_CP)
 *   ├── CUUP  --[NS_CONTAINS]──  (subType=MACRO_SUB6_CU_UP)
 *   └── DU    --[NS_CONTAINS]──  (subType=MACRO_SUB6_DU)
 *
 *   CUCP  --[PARENT_OF]--> CUUP
 *   CUCP  --[PARENT_OF]--> DU
 *
 * connect.CNF.out  → parent node   (CUUP/DU → CUCP,  CUCP → NS)
 * connect.CNF.in   → child nodes   (CUCP → [CUUP,DU], NS → [CUCP,CUUP,DU])
 */
@Node("CnfNode")
@JsonIgnoreProperties(ignoreUnknown = true)
public class Cnf {

    @Id
    private String cnfId;

    private String name;

    /** "CNF" or "NetworkService" — maps to @type in inventory payload */
    private String entityType;

    /**
     * For CNF  : MACRO_SUB6_CU_CP | MACRO_SUB6_CU_UP | MACRO_SUB6_DU | NGDU …
     * For NS   : GnodeB Sub6 | GnodeB NGDU …
     */
    private String subType;

    /** CU-CP | CU-UP | DU  (from cnfneType characteristic, CNF only) */
    private String neType;

    /** LCM/operational status */
    private String currentState;

    /** technology field from characteristics (e.g. "T5G") */
    private String technology;

    /** vendor (e.g. "ALTIOSTAR") */
    private String vendor;

    /**
     * Full entityCharacteristic stored as JSON string.
     * Deserialized in EntityResponse when returned via API.
     */
    private String characteristics;

    // ── Relationships ──────────────────────────────────────────

    /** CUCP --[PARENT_OF]--> CUUP, DU  (connect.CNF.in for CUCP) */
    @Relationship(type = "PARENT_OF")
    private List<Cnf> children;

    /** NS --[NS_CONTAINS]--> CUCP, CUUP, DU  (connect.CNF.in for NS) */
    @Relationship(type = "NS_CONTAINS")
    private List<Cnf> nsCnfs;

    /** Legacy DEPENDS_ON — backward compat */
    @Relationship(type = "DEPENDS_ON")
    private List<Cnf> dependencies;

    public Cnf() {}

    public Cnf(String cnfId, String name, String entityType, String subType,
               String neType, String currentState) {
        this.cnfId = cnfId;
        this.name = name;
        this.entityType = entityType;
        this.subType = subType;
        this.neType = neType;
        this.currentState = currentState;
    }

    public String getCnfId() { return cnfId; }
    public void setCnfId(String cnfId) { this.cnfId = cnfId; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getEntityType() { return entityType; }
    public void setEntityType(String entityType) { this.entityType = entityType; }

    public String getSubType() { return subType; }
    public void setSubType(String subType) { this.subType = subType; }

    public String getNeType() { return neType; }
    public void setNeType(String neType) { this.neType = neType; }

    public String getCurrentState() { return currentState; }
    public void setCurrentState(String currentState) { this.currentState = currentState; }

    public String getTechnology() { return technology; }
    public void setTechnology(String technology) { this.technology = technology; }

    public String getVendor() { return vendor; }
    public void setVendor(String vendor) { this.vendor = vendor; }

    public String getCharacteristics() { return characteristics; }
    public void setCharacteristics(String characteristics) { this.characteristics = characteristics; }

    public List<Cnf> getChildren() { return children; }
    public void setChildren(List<Cnf> children) { this.children = children; }

    public List<Cnf> getNsCnfs() { return nsCnfs; }
    public void setNsCnfs(List<Cnf> nsCnfs) { this.nsCnfs = nsCnfs; }

    public List<Cnf> getDependencies() { return dependencies; }
    public void setDependencies(List<Cnf> dependencies) { this.dependencies = dependencies; }

    // ── backward compat getters used by old code ──────────────
    /** @deprecated use getSubType() */
    public String getType() { return subType; }
    /** @deprecated use setSubType() */
    public void setType(String type) { this.subType = type; }
}
