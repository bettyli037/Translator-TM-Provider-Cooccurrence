package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.networknt.schema.*;

import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

public class Validator {

    private static JsonSchema queryGraphSchema;
    private static JsonSchema knowledgeGraphSchema;
    private static JsonSchema resultsSchema;

    static {
        populateJsonSchemas(null);
    }

    public static void populateJsonSchemas(String uri) {
        String specUri = "https://raw.githubusercontent.com/NCATSTranslator/ReasonerAPI/v1.3.0/TranslatorReasonerAPI.yaml";
        if (uri != null && !uri.isBlank()) {
            specUri = uri;
        }
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            JsonSchemaFactory factory = JsonSchemaFactory
                    .builder(JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7))
                    .objectMapper(mapper)
                    .build();
            YAMLMapper yamlMapper = new YAMLMapper();
            JsonNode trapiNode = yamlMapper.readTree(new URL(specUri));
            JsonNode components = trapiNode.get("components").get("schemas");

            ObjectNode componentCopy1 = components.deepCopy();
            ObjectNode qGraphNode = componentCopy1.remove("QueryGraph").deepCopy();
            ObjectNode subcomponents1 = mapper.createObjectNode();
            subcomponents1.set("schemas", componentCopy1);
            qGraphNode.set("components", subcomponents1);
            queryGraphSchema = factory.getSchema(qGraphNode);

            ObjectNode componentCopy2 = components.deepCopy();
            ObjectNode kGraphNode = componentCopy2.remove("KnowledgeGraph").deepCopy();
            ObjectNode subcomponents2 = mapper.createObjectNode();
            subcomponents2.set("schemas", componentCopy2);
            kGraphNode.set("components", subcomponents2);
            knowledgeGraphSchema = factory.getSchema(kGraphNode);

            ObjectNode componentCopy3 = components.deepCopy();
            ObjectNode resultsNode = componentCopy3.remove("QueryGraph").deepCopy();
            ObjectNode subcomponents3 = mapper.createObjectNode();
            subcomponents3.set("schemas", componentCopy3);
            resultsNode.set("components", subcomponents3);
            resultsSchema = factory.getSchema(resultsNode);
        } catch (IOException ex) {
            System.out.println("Could not retrieve TRAPI YAML");
        }
    }
    public Set<ValidationMessage> validateInput(JsonNode message) {
        if (message.has("query_graph")) {
            return queryGraphSchema.validate(message.get("query_graph"));
        }
        return queryGraphSchema.validate(message);
    }

    public Set<ValidationMessage> validateOutput(JsonNode message) {
        Set<ValidationMessage> results = new HashSet<>();
        if (message.has("query_graph")) {
            results.addAll(queryGraphSchema.validate(message.get("query_graph")));
        } else {
            results.add(ValidationMessage.of("missing", CustomErrorMessageType.of("bleh") , "$.query_graph", "#/QueryGraph"));
        }
        if (message.has("knowledge_graph")) {
            results.addAll(knowledgeGraphSchema.validate(message.get("knowledge_graph")));
        } else {
            results.add(ValidationMessage.of("missing", CustomErrorMessageType.of("bleh") , "$.knowledge_graph", "#/KnowledgeGraph"));
        }
        if (message.has("results")) {
            results.addAll(resultsSchema.validate(message.get("results")));
        } else {
            results.add(ValidationMessage.of("missing", CustomErrorMessageType.of("bleh") , "$.results", "#/Results"));
        }
        return results;
    }
}
