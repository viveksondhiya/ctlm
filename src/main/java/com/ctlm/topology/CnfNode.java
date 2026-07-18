package com.ctlm.topology;

import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Relationship;

import java.util.List;

@Node("CnfNode")
public class CnfNode {

    @Id
    private String cnfId;

    private String name;

    private String type;

    private String currentState;

    @Relationship(type = "DEPENDS_ON")
    private List<CnfNode> dependencies;

    public CnfNode() {}

    public CnfNode(String cnfId, String name, String type, String currentState) {
        this.cnfId = cnfId;
        this.name = name;
        this.type = type;
        this.currentState = currentState;
    }

    public String getCnfId() {
        return cnfId;
    }

    public void setCnfId(String cnfId) {
        this.cnfId = cnfId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCurrentState() {
        return currentState;
    }

    public void setCurrentState(String currentState) {
        this.currentState = currentState;
    }

    public List<CnfNode> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<CnfNode> dependencies) {
        this.dependencies = dependencies;
    }
}
