package com.ctlm.topology;

import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.data.neo4j.repository.query.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface CnfNodeRepository extends Neo4jRepository<Cnf, String> {

    Optional<Cnf> findByCnfId(String cnfId);

    // ── connect.CNF.out ─────────────────────────────────────────

    /**
     * Returns the CUCP that is the direct parent of a CNF (PARENT_OF incoming).
     * Used for CUUP and DU → returns CUCP.
     */
    @Query("MATCH (p:CnfNode)-[:PARENT_OF]->(c:CnfNode {cnfId: $cnfId}) RETURN p")
    Optional<Cnf> findDirectParent(@Param("cnfId") String cnfId);

    /**
     * Returns the NS that contains this CNF (NS_CONTAINS incoming).
     * Used for CUCP → returns the NS super parent.
     */
    @Query("MATCH (ns:CnfNode {entityType: 'NetworkService'})-[:NS_CONTAINS]->(c:CnfNode {cnfId: $cnfId}) RETURN ns")
    Optional<Cnf> findNsParent(@Param("cnfId") String cnfId);

    // ── connect.CNF.in ──────────────────────────────────────────

    /**
     * Returns all CNFs directly under an NS (NS_CONTAINS outgoing).
     * Used for NS → returns [CUCP, CUUP, DU].
     */
    @Query("MATCH (ns:CnfNode {cnfId: $nsId})-[:NS_CONTAINS]->(c:CnfNode) RETURN c")
    List<Cnf> findNsCnfs(@Param("nsId") String nsId);

    // ── NS listing ───────────────────────────────────────────────

    /**
     * Returns all NS (NetworkService) nodes.
     */
    @Query("MATCH (n:CnfNode {entityType: 'NetworkService'}) RETURN n")
    List<Cnf> findAllNs();

    /**
     * Find nodes by subType, e.g. all MACRO_SUB6_CU_CP nodes.
     */
    @Query("MATCH (n:CnfNode {subType: $subType}) RETURN n")
    List<Cnf> findBySubType(@Param("subType") String subType);
}
