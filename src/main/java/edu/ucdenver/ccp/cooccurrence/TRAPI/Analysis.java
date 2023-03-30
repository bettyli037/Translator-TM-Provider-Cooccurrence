package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class Analysis {
    // region Properties
    private String reasonerId;
    private Double score;
    private String scoringMethod;
    private final Map<String, List<EdgeBinding>> edgeBindings;
    private final List<AuxiliaryGraph> supportGraphs;
    private final List<Attribute> attributes;
    private final Map<String, JsonNode> additionalProperties;

    // endregion

    // region Boilerplate
    public Analysis() {
        reasonerId = "";
        edgeBindings = new HashMap<>();
        supportGraphs = new ArrayList<>();
        attributes = new ArrayList<>();
        additionalProperties = new HashMap<>();
    }

    public String getReasonerId() {
        return reasonerId;
    }

    public void setReasonerId(String reasonerId) {
        this.reasonerId = reasonerId;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public String getScoringMethod() {
        return scoringMethod;
    }

    public void setScoringMethod(String scoringMethod) {
        this.scoringMethod = scoringMethod;
    }

    public Map<String, List<EdgeBinding>> getEdgeBindings() {
        return edgeBindings;
    }

    public void addEdgeBinding(String key, EdgeBinding value) {
        if (!edgeBindings.containsKey(key)) {
            edgeBindings.put(key, new ArrayList<>());
        }
        edgeBindings.get(key).add(value);
    }

    public List<AuxiliaryGraph> getSupportGraphs() {
        return supportGraphs;
    }

    public void addSupportGraph(AuxiliaryGraph graph) {
        supportGraphs.add(graph);
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public void addAttribute(Attribute attribute) {
        attributes.add(attribute);
    }

    public Map<String, JsonNode> getAdditionalProperties() {
        return additionalProperties;
    }

    public void addAdditionalProperty(String key, JsonNode value) {
        additionalProperties.put(key, value);
    }
    // endregion

    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode json = om.createObjectNode();
        json.put("reasoner_id", this.reasonerId);
        ObjectNode edgeBindingsNode = om.createObjectNode();
        for (Map.Entry<String, List<EdgeBinding>> binding : this.edgeBindings.entrySet()) {
            ArrayNode bindings = om.createArrayNode();
            for (EdgeBinding edgeBinding : binding.getValue()) {
                bindings.add(edgeBinding.toJSON());
            }
            edgeBindingsNode.set(binding.getKey(), bindings);
        }
        json.set("edge_bindings", edgeBindingsNode);
        if (this.score != null) {
            json.put("score", this.score);
        }
        if (this.scoringMethod != null) {
            json.put("scoring_method", this.scoringMethod);
        }
        if (this.supportGraphs.size() > 0) {
            ArrayNode graphArray = om.createArrayNode();
            for (AuxiliaryGraph graph : this.supportGraphs) {
                graphArray.add(graph.toJSON());
            }
            json.set("support_graphs", graphArray);
        }
        if (this.attributes.size() > 0) {
            ArrayNode attributesArray = om.createArrayNode();
            for (Attribute attribute : this.attributes) {
                attributesArray.add(attribute.toJSON());
            }
            json.set("attributes", attributesArray);
        }
        if (this.additionalProperties.size() > 0) {
            for (Map.Entry<String, JsonNode> property : this.additionalProperties.entrySet()) {
                json.set(property.getKey(), property.getValue());
            }
        }
        return json;
    }

    public static Analysis parseJSON(JsonNode json) {
        if (!json.hasNonNull("reasoner_id") || !json.hasNonNull("edge_bindings")) {
            return null;
        }
        Analysis analysis = new Analysis();
        analysis.setReasonerId(json.get("reasoner_id").asText());

        JsonNode edgeNode = json.get("edge_bindings");
        Iterator<String> keyIterator = edgeNode.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            JsonNode edges = edgeNode.get(key);
            Iterator<JsonNode> edgesIterator = edges.elements();
            while (edgesIterator.hasNext()) {
                EdgeBinding edgeBinding = EdgeBinding.parseJSON(edgesIterator.next());
                analysis.addEdgeBinding(key, edgeBinding);
            }
        }

        if (json.hasNonNull("support_graphs") && json.get("support_graphs").isArray()) {
            Iterator<JsonNode> graphsIterator = json.get("support_graphs").elements();
            while (graphsIterator.hasNext()) {
                AuxiliaryGraph graph = AuxiliaryGraph.parseJSON(graphsIterator.next());
                if (graph != null) {
                    analysis.addSupportGraph(graph);
                }
            }
        }

        if (json.hasNonNull("attributes") && json.get("attributes").isArray()) {
            Iterator<JsonNode> attributesIterator = json.get("attributes").elements();
            while (attributesIterator.hasNext()) {
                Attribute attribute = Attribute.parseJSON(attributesIterator.next());
                if (attribute != null) {
                    analysis.addAttribute(attribute);
                }
            }
        }

        keyIterator = json.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("reasoner_id") || key.equals("edge_bindings") || key.equals("support_graphs") || key.equals("attributes")) {
                continue;
            }
            analysis.addAdditionalProperty(key, json.get(key));
        }
        return analysis;
    }
}
