package com.ctlm.topology;

import org.springframework.data.neo4j.repository.Neo4jRepository;

import java.util.Optional;

public interface CnfNodeRepository extends Neo4jRepository<CnfNode, String> {

    Optional<CnfNode> findByCnfId(String cnfId);
}
