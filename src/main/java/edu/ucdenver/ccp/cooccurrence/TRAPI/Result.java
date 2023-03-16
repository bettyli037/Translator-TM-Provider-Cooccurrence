package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class Result {

    private final Map<String, List<NodeBinding>> nodeBindings;

    private final Map<String, List<EdgeBinding>> edgeBindings;
    private Double score;
    private final Map<String, JsonNode> additionalProperties;

    public Result() {
        nodeBindings = new HashMap<>();
        edgeBindings = new HashMap<>();
        additionalProperties = new HashMap<>();
    }

    public Result(Map<String, List<NodeBinding>> nodeBindings, Map<String, List<EdgeBinding>> edgeBindings) {
        this.nodeBindings = nodeBindings;
        this.edgeBindings = edgeBindings;
        additionalProperties = new HashMap<>();
    }

    public Map<String, List<NodeBinding>> getNodeBindings() {
        return nodeBindings;
    }

    public Map<String, List<EdgeBinding>> getEdgeBindings() {
        return edgeBindings;
    }

    public void addEdgeBinding(String key, EdgeBinding value) {
        if (edgeBindings.containsKey(key)) {
            edgeBindings.get(key).add(value);
        } else {
            List<EdgeBinding> valueList = new ArrayList<>();
            valueList.add(value);
            edgeBindings.put(key, valueList);
        }
    }

    public void addNodeBinding(String key, NodeBinding value) {
        if (nodeBindings.containsKey(key)) {
            nodeBindings.get(key).add(value);
        } else {
            List<NodeBinding> valueList = new ArrayList<>();
            valueList.add(value);
            nodeBindings.put(key, valueList);
        }
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Map<String, JsonNode> getAdditionalProperties() {
        return additionalProperties;
    }

    public void addAdditionalProperty(String name, JsonNode property) {
        additionalProperties.put(name, property);
    }

    public boolean bindsNodeCurie(String curie) {
        for(Map.Entry<String, List<NodeBinding>> bindings : this.nodeBindings.entrySet()) {
            for (NodeBinding binding : bindings.getValue()) {
                if (binding != null && binding.getId().equals(curie)) {
                    return true;
                }
            }
        }
        return false;
    }

    public JsonNode toJSON() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode resultsNode = mapper.createObjectNode();

        ObjectNode nodeBindingsNode = mapper.createObjectNode();
        for (Map.Entry<String, List<NodeBinding>> nodeBinding : nodeBindings.entrySet()) {
            ArrayNode arrayNode = mapper.createArrayNode();
            for (NodeBinding binding : nodeBinding.getValue()) {
                if (binding != null) {
                    arrayNode.add(binding.toJSON());
                }
            }
            nodeBindingsNode.set(nodeBinding.getKey(), arrayNode);
        }
        resultsNode.set("node_bindings", nodeBindingsNode);

        ObjectNode edgeBindingsNode = mapper.createObjectNode();
        for (Map.Entry<String, List<EdgeBinding>> edgeBinding : edgeBindings.entrySet()) {
            ArrayNode arrayNode = mapper.createArrayNode();
            for (EdgeBinding binding : edgeBinding.getValue()) {
                if (binding != null) {
                    arrayNode.add(binding.toJSON());
                }
            }
            edgeBindingsNode.set(edgeBinding.getKey(), arrayNode);
        }
        resultsNode.set("edge_bindings", edgeBindingsNode);

        if (this.score != null) {
            resultsNode.put("score", this.score);
        }

        for (Map.Entry<String, JsonNode> kv : this.additionalProperties.entrySet()) {
            resultsNode.set(kv.getKey(), kv.getValue());
        }

        return resultsNode;
    }

    public static Result parseJSON(JsonNode resultNode) {
        if (!resultNode.hasNonNull("node_bindings") || !resultNode.hasNonNull("edge_bindings")) {
            return null;
        }
        Result result = new Result();
        JsonNode nodeBindings = resultNode.get("node_bindings");
        Iterator<String> keyIterator = nodeBindings.fieldNames();
        while(keyIterator.hasNext()) {
            String key = keyIterator.next();
            System.out.println(key);
            JsonNode bindings = nodeBindings.get(key);
            Iterator<JsonNode> bindingsIterator = bindings.elements();
            while (bindingsIterator.hasNext()) {
                result.addNodeBinding(key, NodeBinding.parseJSON(bindingsIterator.next()));
            }
        }
        JsonNode edgeBindings = resultNode.get("edge_bindings");
        keyIterator = edgeBindings.fieldNames();
        while(keyIterator.hasNext()) {
            String key = keyIterator.next();
            System.out.println(key);
            JsonNode bindings = edgeBindings.get(key);
            Iterator<JsonNode> bindingsIterator = bindings.elements();
            while (bindingsIterator.hasNext()) {
                result.addEdgeBinding(key, EdgeBinding.parseJSON(bindingsIterator.next()));
            }
        }
        if (resultNode.hasNonNull("score")) {
            result.setScore(resultNode.get("score").asDouble());
        }
        keyIterator = resultNode.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("node_bindings") || key.equals("edge_bindings") || key.equals("score")) {
                continue;
            }
            result.addAdditionalProperty(key, resultNode.get(key));
        }
        return result;
    }
}
