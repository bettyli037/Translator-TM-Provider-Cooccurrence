package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;

public class KnowledgeGraph {
    private Map<String, KnowledgeNode> nodes;
    private Map<String, KnowledgeEdge> edges;

    public KnowledgeGraph() {
        nodes = new HashMap<>();
        edges = new HashMap<>();
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
        return graphNode;
    }
}

