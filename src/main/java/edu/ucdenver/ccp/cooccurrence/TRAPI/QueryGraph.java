package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.validation.constraints.NotNull;
import java.util.*;

public class QueryGraph {
    private Map<String, QueryNode> nodes;
    private Map<String, QueryEdge> edges;

    public QueryGraph() {
        nodes = new HashMap<>();
        edges = new HashMap<>();
    }

    public Map<String, QueryNode> getNodes() {
        return nodes;
    }

    public void setNodes(Map<String, QueryNode> nodes) {
        this.nodes = nodes;
    }

    public void addNode(String key, QueryNode node) {
        nodes.put(key, node);
    }

    public Map<String, QueryEdge> getEdges() {
        return edges;
    }

    public void setEdges(Map<String, QueryEdge> edges) {
        this.edges = edges;
    }

    public void addEdge(String key, QueryEdge edge) {
        edges.put(key, edge);
    }

    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode graphNode = om.createObjectNode();
        ObjectNode nodes = om.createObjectNode();
        ObjectNode edges = om.createObjectNode();
        for (Map.Entry<String, QueryNode> kv : this.nodes.entrySet()) {
            nodes.set(kv.getKey(), kv.getValue().toJSON());
        }
        for (Map.Entry<String, QueryEdge> kv : this.edges.entrySet()) {
            edges.set(kv.getKey(), kv.getValue().toJSON());
        }
        graphNode.set("nodes", nodes);
        graphNode.set("edges", edges);
        return graphNode;
    }

    @NotNull
    public static QueryGraph parseJSON(JsonNode jsonQGraph) {
        QueryGraph graph = new QueryGraph();
        if (!jsonQGraph.hasNonNull("nodes") || !jsonQGraph.hasNonNull("edges")) {
            return graph;
        }
        JsonNode queryNodes = jsonQGraph.get("nodes");
        Iterator<String> keyIterator = queryNodes.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            JsonNode qNode = queryNodes.get(key);
            if (qNode == null || qNode.isNull() || qNode.isEmpty()) {
                return graph;
            }
            QueryNode node = QueryNode.parseJSON(qNode);
            if (node != null) {
                graph.addNode(key, node);
            }
        }
        JsonNode queryEdges = jsonQGraph.get("edges");
        keyIterator = queryEdges.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            QueryEdge edge = QueryEdge.parseJSON(queryEdges.get(key));
            if (edge != null) {
                graph.addEdge(key, edge);
            }
        }
        return graph;
    }
}

