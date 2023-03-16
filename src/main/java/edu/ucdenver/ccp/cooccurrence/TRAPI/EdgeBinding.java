package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class EdgeBinding {
    private String id;
    private final List<Attribute> attributes;
    private final Map<String, JsonNode> additionalProperties;

    public EdgeBinding() {
        id = "";
        attributes = new ArrayList<>();
        additionalProperties = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public void addAdditionalProperty(String name, JsonNode property) {
        additionalProperties.put(name, property);
    }

    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode edge = om.createObjectNode();
        edge.put("id", this.id);
        if (this.attributes.size() > 0) {
            ArrayNode attributesNode = om.createArrayNode();
            for (Attribute attribute : this.attributes) {
                attributesNode.add(attribute.toJSON());
            }
            edge.set("attributes", attributesNode);
        }
        for (Map.Entry<String, JsonNode> kv : this.additionalProperties.entrySet()) {
            edge.set(kv.getKey(), kv.getValue());
        }
        return edge;
    }

    public static EdgeBinding parseJSON(JsonNode edgeNode) {
        if (!edgeNode.hasNonNull("id")) {
            return null;
        }
        EdgeBinding edgeBinding = new EdgeBinding();
        edgeBinding.setId(edgeNode.get("id").asText());
        if (edgeNode.hasNonNull("attributes") && edgeNode.get("attributes").isArray()) {
            Iterator<JsonNode> attributesIterator = edgeNode.get("attributes").elements();
            while (attributesIterator.hasNext()) {
                Attribute attribute = Attribute.parseJSON(attributesIterator.next());
                if (attribute != null) {
                    edgeBinding.addAttribute(attribute);
                }
            }
        }
        Iterator<String> keyIterator = edgeNode.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("id") || key.equals("attributes")) {
                continue;
            }
            edgeBinding.addAdditionalProperty(key, edgeNode.get(key));
        }
        return edgeBinding;

    }
}
