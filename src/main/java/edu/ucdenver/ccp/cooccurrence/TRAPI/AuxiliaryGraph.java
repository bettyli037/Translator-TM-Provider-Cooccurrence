package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class AuxiliaryGraph {
    // region Properties
    private final List<String> edges;
    private final List<Attribute> attributes;
    private final Map<String, JsonNode> additionalProperties;
    // endregion

    // region Boilerplate
    public AuxiliaryGraph() {
        edges = new ArrayList<>();
        attributes = new ArrayList<>();
        additionalProperties = new HashMap<>();
    }

    public List<String> getEdges() {
        return edges;
    }

    public void addEdge(String edge) {
        edges.add(edge);
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
        ArrayNode edgesNode = om.createArrayNode();
        for (String edge : this.edges) {
            edgesNode.add(edge);
        }
        json.set("edges", edgesNode);
        if (this.attributes.size() > 0) {
            ArrayNode attributesNode = om.createArrayNode();
            for (Attribute attribute : this.attributes) {
                attributesNode.add(attribute.toJSON());
            }
            json.set("attributes", attributesNode);
        }
        for (Map.Entry<String, JsonNode> kv : this.additionalProperties.entrySet()) {
            json.set(kv.getKey(), kv.getValue());
        }
        return json;
    }

    public static AuxiliaryGraph parseJSON(JsonNode json) {
        if (!json.hasNonNull("edges")) {
            return null;
        }
        AuxiliaryGraph auxiliaryGraph = new AuxiliaryGraph();
        Iterator<JsonNode> edgesIterator = json.get("edges").elements();
        while (edgesIterator.hasNext()) {
            auxiliaryGraph.addEdge(edgesIterator.next().asText());
        }

        if (json.hasNonNull("attributes") && json.get("attributes").isArray()) {
            Iterator<JsonNode> attributesIterator = json.get("attributes").elements();
            while (attributesIterator.hasNext()) {
                Attribute attribute = Attribute.parseJSON(attributesIterator.next());
                if (attribute != null) {
                    auxiliaryGraph.addAttribute(attribute);
                }
            }
        }

        Iterator<String> keyIterator = json.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("edges") || key.equals("attributes")) {
                continue;
            }
            auxiliaryGraph.addAdditionalProperty(key, json.get(key));
        }
        return auxiliaryGraph;
    }
}
