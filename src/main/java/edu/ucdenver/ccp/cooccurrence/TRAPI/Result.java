package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class Result {

    private final Map<String, Set<String>> nodeBindings;

    private final Map<String, Set<String>> edgeBindings;

    public Result() {
        nodeBindings = new HashMap<>();
        edgeBindings = new HashMap<>();
    }

    public Result(Map<String, Set<String>> nodeBindings, Map<String, Set<String>> edgeBindings) {
        this.nodeBindings = nodeBindings;
        this.edgeBindings = edgeBindings;
    }

    public Map<String, Set<String>> getNodeBindings() {
        return nodeBindings;
    }

    public Map<String, Set<String>> getEdgeBindings() {
        return edgeBindings;
    }

    public void addEdgeBinding(String key, String value) {
        if (edgeBindings.containsKey(key)) {
            edgeBindings.get(key).add(value);
        } else {
            Set<String> valueSet = new HashSet<>();
            valueSet.add(value);
            edgeBindings.put(key, valueSet);
        }
    }

    public void addNodeBinding(String key, String value) {
        if (nodeBindings.containsKey(key)) {
            nodeBindings.get(key).add(value);
        } else {
            Set<String> valueSet = new HashSet<>();
            valueSet.add(value);
            nodeBindings.put(key, valueSet);
        }
    }

    public JsonNode toJSON() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode resultsNode = mapper.createObjectNode();

        ObjectNode nodeBindingsNode = mapper.createObjectNode();
        for (Map.Entry<String, Set<String>> nodeBinding : nodeBindings.entrySet()) {
            ArrayNode idsNode = mapper.createArrayNode();
            for (String id : nodeBinding.getValue()) {
                ObjectNode idNode = mapper.createObjectNode();
                idNode.put("id", id);
                idsNode.add(idNode);
            }
            nodeBindingsNode.set(nodeBinding.getKey(), idsNode);
        }
        resultsNode.set("node_bindings", nodeBindingsNode);

        ObjectNode edgeBindingsNode = mapper.createObjectNode();
        for (Map.Entry<String, Set<String>> edgeBinding : edgeBindings.entrySet()) {
            ArrayNode idsNode = mapper.createArrayNode();
            for (String id : edgeBinding.getValue()) {
                ObjectNode idNode = mapper.createObjectNode();
                idNode.put("id", id);
                idsNode.add(idNode);
            }
            edgeBindingsNode.set(edgeBinding.getKey(), idsNode);
        }
        resultsNode.set("edge_bindings", edgeBindingsNode);

        return resultsNode;
    }
}
