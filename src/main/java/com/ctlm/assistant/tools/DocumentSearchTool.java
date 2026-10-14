package com.ctlm.assistant.tools;

import org.springframework.ai.tool.annotation.Tool;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class DocumentSearchTool {

    private final String docContent;

    public DocumentSearchTool() {
        this.docContent = loadDocs();
    }

    @Tool(description = "Search troubleshooting documentation for known issues and recommended actions " +
            "related to CNF/NS states like INSTANTIATION_PENDING, INSTANTIATION_FAILED, etc.")
    public String searchDocumentation(String query) {
        String[] keywords = query.toLowerCase().split("\\s+");
        String[] paragraphs = docContent.split("---");

        List<String> matches = Arrays.stream(paragraphs)
                .filter(p -> Arrays.stream(keywords).anyMatch(k -> p.toLowerCase().contains(k)))
                .collect(Collectors.toList());

        if (matches.isEmpty()) {
            return "No relevant documentation found for: " + query;
        }

        return "Relevant documentation:\n\n" + String.join("\n---\n", matches);
    }

    private String loadDocs() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("docs/troubleshooting.txt")) {
            if (is == null) return "";
            return new BufferedReader(new InputStreamReader(is))
                    .lines().collect(Collectors.joining("\n"));
        } catch (Exception e) {
            return "";
        }
    }
}

