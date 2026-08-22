package com.ctlm.utils;

import com.ctlm.dto.CnfBlueprint;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CnfBlueprintUtils {

    private static final Map<String, List<CnfBlueprint>> NS_TYPE_TO_CNFS =
            new LinkedHashMap<>();

    static {
        // MacroSub6: CUCP (parent) + CUUP + DU
        NS_TYPE_TO_CNFS.put("GnodeB Sub6", Arrays.asList(
                new CnfBlueprint("MACRO_SUB6_CU_CP", "CU-CP", true),
                new CnfBlueprint("MACRO_SUB6_CU_UP", "CU-UP", false),
                new CnfBlueprint("MACRO_SUB6_DU", "DU", false)
        ));

        // NGDU_Sub6: CUCP (parent) + CUUP + DU
        NS_TYPE_TO_CNFS.put("GnodeB NGDU_Sub6", Arrays.asList(
                new CnfBlueprint("NGDU_SUB6_CU_CP", "CU-CP", true),
                new CnfBlueprint("NGDU_SUB6_CU_UP", "CU-UP", false),
                new CnfBlueprint("NGDU_SUB6_DU", "DU", false)
        ));

        // NGDU_CPRI_Sub6: same CNF structure as NGDU_Sub6
        NS_TYPE_TO_CNFS.put("GnodeB NGDU_CPRI_Sub6", Arrays.asList(
                new CnfBlueprint("NGDU_SUB6_CU_CP", "CU-CP", true),
                new CnfBlueprint("NGDU_SUB6_CU_UP", "CU-UP", false),
                new CnfBlueprint("NGDU_SUB6_DU", "DU", false)
        ));
    }

    private CnfBlueprintUtils() {
        // Utility class - prevent instantiation
    }

    /**
     * Returns the CNF blueprint list for the given NS type.
     */
    public static List<CnfBlueprint> getCnfBlueprints(String nsType) {
        return NS_TYPE_TO_CNFS.getOrDefault(
                nsType,
                Collections.emptyList()
        );
    }

    /**
     * Checks whether the given NS type is supported.
     */
    public static boolean isSupportedNsType(String nsType) {
        return NS_TYPE_TO_CNFS.containsKey(nsType);
    }
}