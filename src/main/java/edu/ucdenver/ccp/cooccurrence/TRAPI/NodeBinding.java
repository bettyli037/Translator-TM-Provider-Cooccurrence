package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class NodeBinding {
    private String id;
    private String queryId;
    private final List<Attribute> attributes;
    private final Map<String, JsonNode> additionalProperties;

    public NodeBinding() {
        id = "";
        queryId = "";
        attributes = new ArrayList<>();
        additionalProperties = new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
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
        ObjectNode node = om.createObjectNode();
        node.put("id", this.id);
        if (this.queryId.length() > 0) {
            node.put("query_id", this.queryId);
        }
        if (this.attributes.size() > 0) {
            ArrayNode attributesNode = om.createArrayNode();
            for (Attribute attribute : this.attributes) {
                attributesNode.add(attribute.toJSON());
            }
            node.set("attributes", attributesNode);
        }
        for (Map.Entry<String, JsonNode> kv : this.additionalProperties.entrySet()) {
            node.set(kv.getKey(), kv.getValue());
        }
        return node;
    }

    public static NodeBinding parseJSON(JsonNode jsonNode) {
        if (!jsonNode.hasNonNull("id")) {
            return null;
        }
        NodeBinding nodeBinding = new NodeBinding();
        nodeBinding.setId(jsonNode.get("id").asText());
        if (jsonNode.hasNonNull("query_id")) {
            nodeBinding.setQueryId(jsonNode.get("query_id").asText());
        }
        if (jsonNode.hasNonNull("attributes") && jsonNode.get("attributes").isArray()) {
            Iterator<JsonNode> attributesIterator = jsonNode.get("attributes").elements();
            while (attributesIterator.hasNext()) {
                Attribute attribute = Attribute.parseJSON(attributesIterator.next());
                if (attribute != null) {
                    nodeBinding.addAttribute(attribute);
                }
            }
        }
        Iterator<String> keyIterator = jsonNode.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("id") || key.equals("query_id") || key.equals("attributes")) {
                continue;
            }
            nodeBinding.addAdditionalProperty(key, jsonNode.get(key));
        }
        return nodeBinding;
    }
}
