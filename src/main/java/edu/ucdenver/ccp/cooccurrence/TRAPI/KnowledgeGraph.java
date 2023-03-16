package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KnowledgeGraph {
    private Map<String, KnowledgeNode> nodes;
    private Map<String, KnowledgeEdge> edges;
    private Map<String, JsonNode> additionalProperties;

    public KnowledgeGraph() {
        nodes = new HashMap<>();
        edges = new HashMap<>();
        additionalProperties = new HashMap<>();
    }

    public Map<String, KnowledgeNode> getNodes() {
        return nodes;
    }

    public void setNodes(Map<String, KnowledgeNode> nodes) {
        this.nodes = nodes;
    }

    public void addNode(String key, KnowledgeNode node) {
        nodes.put(key, node);
    }

    public Map<String, KnowledgeEdge> getEdges() {
        return edges;
    }

    public void setEdges(Map<String, KnowledgeEdge> edges) {
        this.edges = edges;
    }

    public void addEdge(String key, KnowledgeEdge edge) {
        edges.put(key, edge);
    }

    public Map<String, JsonNode> getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Map<String, JsonNode> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public void addAdditionalProperty(String key, JsonNode value) {
        additionalProperties.put(key, value);
    }
    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode graphNode = om.createObjectNode();
        ObjectNode nodes = om.createObjectNode();
        ObjectNode edges = om.createObjectNode();
        for (Map.Entry<String, KnowledgeNode> kv : this.nodes.entrySet()) {
            nodes.set(kv.getKey(), kv.getValue().toJSON());
        }
        for (Map.Entry<String, KnowledgeEdge> kv : this.edges.entrySet()) {
            edges.set(kv.getKey(), kv.getValue().toJSON());
        }
        graphNode.set("nodes", nodes);
        graphNode.set("edges", edges);
        for (Map.Entry<String, JsonNode> kv : this.additionalProperties.entrySet()) {
            graphNode.set(kv.getKey(), kv.getValue());
        }
        return graphNode;
    }

    @NotNull
    public static KnowledgeGraph parseJSON(JsonNode jsonKGraph) {
        KnowledgeGraph graph = new KnowledgeGraph();
        if (!jsonKGraph.hasNonNull("nodes") || !jsonKGraph.hasNonNull("edges")) {
            return graph;
        }
        JsonNode knowledgeNodes = jsonKGraph.get("nodes");
        Iterator<String> keyIterator = knowledgeNodes.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            JsonNode kNode = knowledgeNodes.get(key);
            if (kNode == null || kNode.isNull() || kNode.isEmpty()) {
                return graph;
            }
            graph.addNode(key, KnowledgeNode.parseJSON(kNode));
        }
        JsonNode knowledgeEdges = jsonKGraph.get("edges");
        keyIterator = knowledgeEdges.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            KnowledgeEdge edge = KnowledgeEdge.parseJSON(knowledgeEdges.get(key));
            if (edge != null) {
                graph.addEdge(key, edge);
            }
        }
        keyIterator = jsonKGraph.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("nodes") || key.equals("edges")) {
                continue;
            }
            graph.addAdditionalProperty(key, jsonKGraph.get(key));
        }
        return graph;
    }
}

