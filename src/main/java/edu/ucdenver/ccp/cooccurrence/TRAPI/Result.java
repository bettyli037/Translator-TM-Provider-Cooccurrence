package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class Result {

    private final Map<String, List<NodeBinding>> nodeBindings;
    private final List<Analysis> analyses;
    private final Map<String, JsonNode> additionalProperties;

    public Result() {
        nodeBindings = new HashMap<>();
        analyses = new ArrayList<>();
        additionalProperties = new HashMap<>();
    }

    public Result(Map<String, List<NodeBinding>> nodeBindings, Map<String, List<EdgeBinding>> edgeBindings) {
        this.nodeBindings = nodeBindings;
        additionalProperties = new HashMap<>();
        analyses = new ArrayList<>();
    }

    public Result(Map<String, List<NodeBinding>> nodeBindings, List<Analysis> analyses) {
        this.nodeBindings = nodeBindings;
        this.analyses = analyses;
        this.additionalProperties = new HashMap<>();
    }

    public Map<String, List<NodeBinding>> getNodeBindings() {
        return nodeBindings;
    }

    public List<Analysis> getAnalyses() {
        return analyses;
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

    public void addAnalysis(Analysis analysis) {
        this.analyses.add(analysis);
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

        ArrayNode analysesNode = mapper.createArrayNode();
        for (Analysis analysis : this.analyses) {
            analysesNode.add(analysis.toJSON());
        }
        resultsNode.set("analyses", analysesNode);

        for (Map.Entry<String, JsonNode> kv : this.additionalProperties.entrySet()) {
            resultsNode.set(kv.getKey(), kv.getValue());
        }

        return resultsNode;
    }

    public static Result parseJSON(JsonNode resultNode) {
        if (!resultNode.hasNonNull("node_bindings") || !resultNode.hasNonNull("analyses") || !resultNode.get("analyses").isArray()) {
            return null;
        }
        Result result = new Result();
        JsonNode nodeBindings = resultNode.get("node_bindings");
        Iterator<String> keyIterator = nodeBindings.fieldNames();
        while(keyIterator.hasNext()) {
            String key = keyIterator.next();
            JsonNode bindings = nodeBindings.get(key);
            Iterator<JsonNode> bindingsIterator = bindings.elements();
            while (bindingsIterator.hasNext()) {
                result.addNodeBinding(key, NodeBinding.parseJSON(bindingsIterator.next()));
            }
        }
        Iterator<JsonNode> analysesIterator = resultNode.get("analyses").elements();
        while (analysesIterator.hasNext()) {
            JsonNode nextAnalysis = analysesIterator.next();
            System.out.println(nextAnalysis);
            Analysis attribute = Analysis.parseJSON(nextAnalysis);
            if (attribute != null) {
                result.addAnalysis(attribute);
            }
        }
        keyIterator = resultNode.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("node_bindings") || key.equals("analyses")) {
                continue;
            }
            result.addAdditionalProperty(key, resultNode.get(key));
        }
        return result;
    }
}
