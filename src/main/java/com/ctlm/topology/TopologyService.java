package com.ctlm.topology;

import com.ctlm.dto.EntityResponse;
import com.ctlm.dto.NsTriggerRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TopologyService {

    private final CnfNodeRepository cnfNodeRepository;
    private final ObjectMapper objectMapper;

    // ── NS type → CNF blueprint mapping ─────────────────────────

    /**
     * Defines which CNFs are auto-created for each NS type.
     *
     * Both solutions follow the same 3-CNF hierarchy:
     *   CUCP (isParent=true) --[PARENT_OF]--> CUUP + DU
     *   NS   --[NS_CONTAINS]--> CUCP, CUUP, DU
     *
     * nsType              subType prefix    neTypes
     * "GnodeB Sub6"     → MACRO_SUB6_*     CU-CP / CU-UP / DU
     * "GnodeB NGDU_Sub6"→ NGDU_SUB6_*      CU-CP / CU-UP / DU
     */
    private static final Map<String, List<CnfBlueprint>> NS_TYPE_TO_CNFS = new LinkedHashMap<>();

    static {
        // MacroSub6: CUCP (parent) + CUUP + DU
        NS_TYPE_TO_CNFS.put("GnodeB Sub6", Arrays.asList(
            new CnfBlueprint("MACRO_SUB6_CU_CP", "CU-CP", true),
            new CnfBlueprint("MACRO_SUB6_CU_UP", "CU-UP", false),
            new CnfBlueprint("MACRO_SUB6_DU",    "DU",    false)
        ));

        // NGDU_Sub6: same 3-CNF structure and hierarchy as MacroSub6,
        // different subType prefix (NGDU_SUB6_*).
        NS_TYPE_TO_CNFS.put("GnodeB NGDU_Sub6", Arrays.asList(
            new CnfBlueprint("NGDU_SUB6_CU_CP", "CU-CP", true),
            new CnfBlueprint("NGDU_SUB6_CU_UP", "CU-UP", false),
            new CnfBlueprint("NGDU_SUB6_DU",    "DU",    false)
        ));
    }

    /** Describes one CNF that must be created under an NS */
    private record CnfBlueprint(String subType, String neType, boolean isParent) {}

    // ──────────────────────────────────────────────────────────
    //  NS Trigger — auto-creates NS + child CNFs + relationships
    // ──────────────────────────────────────────────────────────

    /**
     * Main entry point called when IPTX / ZTP fires an NS-create event.
     *
     * Steps:
     *  1. Convert characteristics list → Map<String,String>
     *  2. Create NS node (entityType=NetworkService)
     *  3. Look up blueprints for nsType → create child CNF nodes
     *  4. NS_CONTAINS → all child CNFs
     *  5. PARENT_OF   → CUCP → (CUUP + DU)   [same for both MacroSub6 and NGDU_Sub6]
     *  6. Return NS node
     */
    public Cnf triggerNsCreate(NsTriggerRequest request) {
        Map<String, String> charMap = toCharMap(request.getCharacteristics());

        // 1. Create NS — immediately INSTANTIATION_IN_PROGRESS
        Cnf ns = new Cnf( request.getNsId(),request.getName(),
            "NetworkService",  request.getNsType(),  null,
            "INSTANTIATION_IN_PROGRESS"
        );
        ns.setTechnology(charMap.get("technology"));
        ns.setVendor(charMap.get("vendor"));
        ns.setCharacteristics(toJson(charMap));
        cnfNodeRepository.save(ns);
        log.info("NS created → INSTANTIATION_IN_PROGRESS: id={}, nsType={}", ns.getCnfId(), ns.getSubType());

        // 2. Determine child CNF blueprints
        List<CnfBlueprint> blueprints = NS_TYPE_TO_CNFS.get(request.getNsType());
        if (blueprints == null) {
            throw new IllegalArgumentException(
                "Unknown nsType: '" + request.getNsType() + "'. Supported: " + NS_TYPE_TO_CNFS.keySet()
            );
        }

        // 3. Create CNF nodes
        List<Cnf> allCnfs = new ArrayList<>();
        Cnf parentCnf = null;
        List<Cnf> childCnfs = new ArrayList<>();

        for (CnfBlueprint cnfEntity : blueprints) {
            String cnfId = request.getNsId() + "-" + cnfEntity.subType().toLowerCase().replace("_", "-");
            // Each CNF created immediately as INSTANTIATION_IN_PROGRESS
            Cnf cnf = new Cnf( cnfId,  request.getName(),
                "CNF",  cnfEntity.subType(),  cnfEntity.neType(),
                "INSTANTIATION_IN_PROGRESS"
            );
            cnf.setTechnology(charMap.get("technology"));
            cnf.setVendor(charMap.get("vendor"));
            Map<String, String> cnfCharMap = new LinkedHashMap<>(charMap);
            cnfCharMap.put("subType", cnfEntity.subType());
            cnfCharMap.put("cnfneType", cnfEntity.neType());
            cnf.setCharacteristics(toJson(cnfCharMap));
            cnfNodeRepository.save(cnf);
            log.info("CNF created → INSTANTIATION_IN_PROGRESS: id={}, subType={}", cnfId, cnfEntity.subType());

            allCnfs.add(cnf);
            if (cnfEntity.isParent()) {
                parentCnf = cnf;
            } else {
                childCnfs.add(cnf);
            }
        }

        // 4. NS_CONTAINS → all CNFs
        ns = cnfNodeRepository.findByCnfId(request.getNsId()).orElseThrow();
        ns.setNsCnfs(new ArrayList<>(allCnfs));
        cnfNodeRepository.save(ns);

        // 5. PARENT_OF: parentCnf (CUCP) → childCnfs (CUUP + DU)
        if (parentCnf != null && !childCnfs.isEmpty()) {
            Cnf parent = cnfNodeRepository.findByCnfId(parentCnf.getCnfId()).orElseThrow();
            parent.setChildren(new ArrayList<>(childCnfs));
            cnfNodeRepository.save(parent);
            log.info("PARENT_OF: {} -> {}", parent.getCnfId(),
                childCnfs.stream().map(Cnf::getCnfId).collect(Collectors.joining(", ")));
        }

        return cnfNodeRepository.findByCnfId(request.getNsId()).orElseThrow();
    }

    // ──────────────────────────────────────────────────────────
    //  Unified entity lookup — works for both NS and CNF
    // ──────────────────────────────────────────────────────────

    /**
     * GET /api/topology/entity/{id}
     * Returns full entity data + connect.out (parent) + connect.in (children).
     */
    public EntityResponse getEntityById(String id) {
        Cnf node = cnfNodeRepository.findByCnfId(id)
            .orElseThrow(() -> new IllegalArgumentException("Entity not found: " + id));

        EntityResponse resp = new EntityResponse();
        resp.setId(node.getCnfId());
        resp.setEntityType(node.getEntityType());
        resp.setName(node.getName());
        resp.setSubType(node.getSubType());
        resp.setNeType(node.getNeType());
        resp.setCurrentState(node.getCurrentState());
        resp.setTechnology(node.getTechnology());
        resp.setVendor(node.getVendor());
        resp.setCharacteristics(fromJson(node.getCharacteristics()));

        // connect.CNF.out → parent
        connectOut(id).ifPresent(parent ->
            resp.setConnectOut(toRef(parent))
        );

        // connect.CNF.in → children
        List<Cnf> children = connectIn(id);
        resp.setConnectIn(children.stream().map(this::toRef).collect(Collectors.toList()));

        return resp;
    }

    // ──────────────────────────────────────────────────────────
    //  connect.CNF.out / connect.CNF.in
    // ──────────────────────────────────────────────────────────

    /**
     * connect.CNF.out — parent of the given node.
     *   CUUP / DU → CUCP   (via PARENT_OF incoming)
     *   CUCP      → NS     (via NS_CONTAINS incoming)
     *   NS        → empty
     */
    public Optional<Cnf> connectOut(String cnfId) {
        Optional<Cnf> direct = cnfNodeRepository.findDirectParent(cnfId);
        if (direct.isPresent()) {
            log.info("connect.out {} → {} (PARENT_OF)", cnfId, direct.get().getCnfId());
            return direct;
        }
        Optional<Cnf> ns = cnfNodeRepository.findNsParent(cnfId);
        ns.ifPresent(n -> log.info("connect.out {} → {} (NS_CONTAINS)", cnfId, n.getCnfId()));
        return ns;
    }

    /**
     * connect.CNF.in — children of the given node.
     *   NS   → [CUCP, CUUP, DU]  (via NS_CONTAINS)
     *   CUCP → [CUUP, DU]        (via PARENT_OF)
     *   CUUP / DU → []
     */
    public List<Cnf> connectIn(String cnfId) {
        Cnf node = cnfNodeRepository.findByCnfId(cnfId)
            .orElseThrow(() -> new IllegalArgumentException("Entity not found: " + cnfId));

        if ("NetworkService".equals(node.getEntityType())) {
            List<Cnf> cnfs = cnfNodeRepository.findNsCnfs(cnfId);
            log.info("connect.in NS {} → {} CNFs", cnfId, cnfs.size());
            return cnfs;
        }

        List<Cnf> children = node.getChildren() != null ? node.getChildren() : new ArrayList<>();
        log.info("connect.in {} → {} children", cnfId, children.size());
        return children;
    }

    // ──────────────────────────────────────────────────────────
    //  Existing methods (unchanged)
    // ──────────────────────────────────────────────────────────

    public Cnf registerCnf(String cnfId, String name, String type) {
        Cnf node = new Cnf(cnfId, name, "CNF", type, null, "UNINSTANTIATED");
        log.info("Registering CNF: id={}, name={}, subType={}", cnfId, name, type);
        return cnfNodeRepository.save(node);
    }

    public void updateState(String cnfId, String newState) {
        cnfNodeRepository.findByCnfId(cnfId).ifPresent(node -> {
            node.setCurrentState(newState);
            cnfNodeRepository.save(node);
            log.info("Updated state for {} → {}", cnfId, newState);
        });
    }

    public Optional<Cnf> getCnf(String cnfId) {
        return cnfNodeRepository.findByCnfId(cnfId);
    }

    public Cnf registerNs(String nsId, String name) {
        Cnf ns = new Cnf(nsId, name, "NetworkService", null, null, "UNINSTANTIATED");
        log.info("Registering NS: id={}, name={}", nsId, name);
        return cnfNodeRepository.save(ns);
    }

    public void addNsCnf(String nsId, String cnfId) {
        Cnf ns = cnfNodeRepository.findByCnfId(nsId)
            .orElseThrow(() -> new IllegalArgumentException("NS not found: " + nsId));
        if (!"NetworkService".equals(ns.getEntityType())) {
            throw new IllegalArgumentException(nsId + " is not a NetworkService node");
        }
        Cnf cnf = cnfNodeRepository.findByCnfId(cnfId)
            .orElseThrow(() -> new IllegalArgumentException("CNF not found: " + cnfId));
        List<Cnf> list = ns.getNsCnfs() != null ? new ArrayList<>(ns.getNsCnfs()) : new ArrayList<>();
        list.add(cnf);
        ns.setNsCnfs(list);
        cnfNodeRepository.save(ns);
        log.info("NS_CONTAINS: {} → {}", nsId, cnfId);
    }

    public void addChild(String parentCnfId, String childCnfId) {
        Cnf parent = cnfNodeRepository.findByCnfId(parentCnfId)
            .orElseThrow(() -> new IllegalArgumentException("Parent not found: " + parentCnfId));
        Cnf child = cnfNodeRepository.findByCnfId(childCnfId)
            .orElseThrow(() -> new IllegalArgumentException("Child not found: " + childCnfId));
        List<Cnf> children = parent.getChildren() != null ? new ArrayList<>(parent.getChildren()) : new ArrayList<>();
        children.add(child);
        parent.setChildren(children);
        cnfNodeRepository.save(parent);
        log.info("PARENT_OF: {} → {}", parentCnfId, childCnfId);
    }

    public void addDependency(String cnfId, String dependsOnCnfId) {
        Cnf node = cnfNodeRepository.findByCnfId(cnfId)
            .orElseThrow(() -> new IllegalArgumentException("CNF not found: " + cnfId));
        Cnf dep = cnfNodeRepository.findByCnfId(dependsOnCnfId)
            .orElseThrow(() -> new IllegalArgumentException("Dependency not found: " + dependsOnCnfId));
        List<Cnf> deps = node.getDependencies() != null ? new ArrayList<>(node.getDependencies()) : new ArrayList<>();
        deps.add(dep);
        node.setDependencies(deps);
        cnfNodeRepository.save(node);
        log.info("DEPENDS_ON: {} → {}", cnfId, dependsOnCnfId);
    }

    public List<Cnf> getDependencies(String cnfId) {
        return cnfNodeRepository.findByCnfId(cnfId)
            .map(n -> n.getDependencies() != null ? n.getDependencies() : new ArrayList<Cnf>())
            .orElse(new ArrayList<>());
    }

    public List<Cnf> getAllNs() {
        return cnfNodeRepository.findAllNs();
    }

    // ──────────────────────────────────────────────────────────
    //  Private helpers
    // ──────────────────────────────────────────────────────────

    private Map<String, String> toCharMap(List<NsTriggerRequest.CharacteristicEntry> entries) {
        if (entries == null) return new LinkedHashMap<>();
        Map<String, String> map = new LinkedHashMap<>();
        entries.forEach(e -> map.put(e.getName(), e.getValue()));
        return map;
    }

    private String toJson(Map<String, String> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize characteristics", e);
            return "{}";
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> fromJson(String json) {
        if (json == null || json.isBlank()) return new LinkedHashMap<>();
        try {
            return objectMapper.readValue(json, Map.class);
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize characteristics", e);
            return new LinkedHashMap<>();
        }
    }

    private EntityResponse.ConnectRef toRef(Cnf node) {
        return new EntityResponse.ConnectRef(
            node.getCnfId(),
            node.getName(),
            node.getEntityType(),
            node.getSubType(),
            node.getNeType()
        );
    }
}
