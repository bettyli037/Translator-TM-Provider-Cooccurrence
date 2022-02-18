package edu.ucdenver.ccp.cooccurrence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

@RestController
public class CooccurrenceController {

    private final NodeRepository nodeRepo;
    private final ObjectMapper objectMapper;

    public CooccurrenceController(NodeRepository repo) {
        this.nodeRepo = repo;
        this.objectMapper = new ObjectMapper();
    }

    @GetMapping("/hierarchy/{curie}")
    public String getHierarchy(@PathVariable("curie") String curie) {
        List<String> ancestors = getAllAncestors(curie);
        ObjectMapper om = new ObjectMapper();
        ObjectNode responseNode = om.createObjectNode();
        ArrayNode ancestorNode = responseNode.putArray("hierarchy");
        for (String ancestor : ancestors) {
            ancestorNode.add(ancestor);
        }
        return responseNode.toPrettyString();
    }

    @PostMapping("/metrics")
    public String calculateMetrics(@RequestBody JsonNode request) {
        if (!(request.hasNonNull("concept1") && request.hasNonNull("concept2") && request.hasNonNull("document_part"))) {
            return "Invalid Request";
        }
        long startTime = System.currentTimeMillis();
        String concept1 = request.get("concept1").asText();
        String concept2 = request.get("concept2").asText();
        String part = request.get("document_part").asText();

        List<String> concept1List = getAllAncestors(concept1);
        List<String> concept2List = getAllAncestors(concept2);
        JsonNode metricsNode = getMetrics(concept1List, concept2List, part).toJSON();
        ObjectMapper om = new ObjectMapper();
        ObjectNode responseNode = om.createObjectNode();
        ObjectNode expandedConceptsNode = om.createObjectNode();
        responseNode.set("request", request);

        ArrayNode concept1Node = expandedConceptsNode.putArray("concept1");
        for (String ancestor : concept1List) {
            concept1Node.add(ancestor);
        }
        ArrayNode concept2Node = expandedConceptsNode.putArray("concept2");
        for (String ancestor : concept2List) {
            concept2Node.add(ancestor);
        }
        responseNode.set("expandedConcepts", expandedConceptsNode);
        responseNode.set("metrics", metricsNode);
        long endTime = System.currentTimeMillis();
        responseNode.put("elapsed_time", endTime - startTime);
        return responseNode.toPrettyString();
    }

    @GetMapping("/meta_knowledge_graph")
    public JsonNode getMetaKnowledgeGraph() {
        List<NodeMetadata> nodeMetadata = nodeRepo.getNodeMetadata();
        List<EdgeMetadata> edgeMetadata = nodeRepo.getEdgeMetadata();

        ObjectNode nodeMetadataNode = objectMapper.createObjectNode();
        Map<String, List<String>> categoryPrefixMap = new HashMap<>();
        for (NodeMetadata nm : nodeMetadata) {
            if (!categoryPrefixMap.containsKey(nm.getCategory())) {
                categoryPrefixMap.put(nm.getCategory(), new ArrayList<>());
            }
            categoryPrefixMap.get(nm.getCategory()).add(nm.getIdPrefix());
        }
        for (Map.Entry<String, List<String>> categoryEntry : categoryPrefixMap.entrySet()) {
            ObjectNode entry = objectMapper.createObjectNode();
            ArrayNode prefixes = objectMapper.valueToTree(categoryEntry.getValue());
            entry.set("id_prefixes", prefixes);
            entry.set("attributes", objectMapper.nullNode());
            nodeMetadataNode.set(categoryEntry.getKey(), entry);
        }

        ArrayNode edgeMetadataArray = objectMapper.createArrayNode();
        for (EdgeMetadata em : edgeMetadata) {
            ObjectNode entry = objectMapper.createObjectNode();
            entry.put("subject", em.getSubject());
            entry.put("object", em.getObject());
            entry.put("predicate", em.getPredicate());
            entry.set("attributes", objectMapper.nullNode());
            edgeMetadataArray.add(entry);
        }

        ObjectNode responseNode = objectMapper.createObjectNode();
        responseNode.set("nodes", nodeMetadataNode);
        responseNode.set("edges", edgeMetadataArray);
        return responseNode;
    }

    @PostMapping("/overlay")
    public ResponseEntity<JsonNode> overlay(@RequestBody ObjectNode request) {
        ObjectMapper om = new ObjectMapper();
        if (!isValid(request)) {
            // TODO: figure out what caused it to fail validation and use it to build a ValidationError object
            return ResponseEntity.unprocessableEntity().body(om.createObjectNode().put("error", "Validation failed"));
        }
        JsonNode nodes = request.get("message").get("knowledge_graph").get("nodes");
        ObjectNode newEdgesNode = om.createObjectNode();
        List<String> concepts = new ArrayList<>();
        ArrayNode newEdgeBindings = om.createArrayNode();
        for (Iterator<String> iter = nodes.fieldNames(); iter.hasNext(); ) {
            concepts.add(iter.next());
        }
        for (List<String> pair : getAllConceptPairs(concepts)) {
            String s = pair.get(0);
            String o = pair.get(1);
            List<String> concept1List = getAllAncestors(s);
            List<String> concept2List = getAllAncestors(o);
            Metrics cooccurrenceMetrics = getMetrics(concept1List, concept2List, "abstract");
            if (Double.isNaN(cooccurrenceMetrics.getNormalizedGoogleDistance())) {
                continue;
            }
            String edgeID = s + "_" + o + "_abstract";
            ObjectNode binding = om.createObjectNode();
            binding.put("id", edgeID);
            newEdgeBindings.add(binding);
            ObjectNode newEdge = om.createObjectNode();
            ObjectNode edgeAttributes = om.createObjectNode();
            newEdge.put("subject", s);
            newEdge.put("object", o);
            newEdge.put("predicate", "biolink:occurs_together_in_literature_with");
            edgeAttributes.put("attribute_type_id", "biolink:supporting_study_result");
            edgeAttributes.put("value", "tmkp:" + edgeID);
            edgeAttributes.put("value_type_id", "biolink:AbstractLevelConceptCooccurrenceAnalysisResult");
            edgeAttributes.put("description", "a single result from computing cooccurrence metrics between two concepts that occur in the document abstract");
            edgeAttributes.put("attribute_source", "infores:text-mining-provider-cooccurrence");
            edgeAttributes.set("attributes", cooccurrenceMetrics.toJSONArray());
            newEdge.set("attributes", edgeAttributes);
            newEdgesNode.set(edgeID, newEdge);
        }
        JsonNode responseNode = om.createObjectNode().set("message", updateMessageNode(request, newEdgesNode, newEdgeBindings));
        return ResponseEntity.ok(responseNode);
    }

    private List<String> getAllAncestors(String initialConcept) {
        List<String> allAncestors = new ArrayList<>();
        allAncestors.add(initialConcept);
        List<String> nullList = Collections.singletonList(null);
        List<String> ancestors = nodeRepo.getConceptAncestors(Collections.singletonList(initialConcept)).stream().distinct().collect(Collectors.toList());
        ancestors.removeAll(nullList);
        while (ancestors.size() > 0) {
            allAncestors.addAll(ancestors);
            ancestors = nodeRepo.getConceptAncestors(ancestors).stream().distinct().collect(Collectors.toList());
            ancestors.removeAll(nullList);
            ancestors.removeAll(allAncestors);
        }
        return allAncestors;
    }

    private Metrics getMetrics(List<String> concept1List, List<String> concept2List, String part) {
        int singleCount1, singleCount2, pairCount, totalConceptCount, totalDocumentCount;
        totalConceptCount = nodeRepo.getTotalConceptCount();
        switch (part) {
            case "abstract":
                singleCount1 = nodeRepo.getSingleConceptHierarchyAbstractCount(concept1List);
                singleCount2 = nodeRepo.getSingleConceptHierarchyAbstractCount(concept2List);
                pairCount = nodeRepo.getPairConceptHierarchyAbstractCount(concept1List, concept2List);
                totalDocumentCount = nodeRepo.getTotalAbstractCount();
                break;
            case "sentence":
                singleCount1 = nodeRepo.getSingleConceptHierarchySentenceCount(concept1List);
                singleCount2 = nodeRepo.getSingleConceptHierarchySentenceCount(concept2List);
                pairCount = nodeRepo.getPairConceptHierarchySentenceCount(concept1List, concept2List);
                totalDocumentCount = nodeRepo.getTotalSentenceCount();
                break;
            case "title":
                singleCount1 = nodeRepo.getSingleConceptHierarchyTitleCount(concept1List);
                singleCount2 = nodeRepo.getSingleConceptHierarchyTitleCount(concept2List);
                pairCount = nodeRepo.getPairConceptHierarchyTitleCount(concept1List, concept2List);
                totalDocumentCount = nodeRepo.getTotalTitleCount();
                break;
            default:
                singleCount1 = nodeRepo.getSingleConceptHierarchyCount(concept1List, part);
                singleCount2 = nodeRepo.getSingleConceptHierarchyCount(concept2List, part);
                pairCount = nodeRepo.getPairConceptHierarchyCount(concept1List, concept2List, part);
                totalDocumentCount = nodeRepo.getTotalDocumentCount();
                break;
        }
        return new Metrics(singleCount1, singleCount2, pairCount, totalConceptCount, totalDocumentCount);
    }

    private JsonNode updateMessageNode(JsonNode messageNode, JsonNode newEdges, ArrayNode newEdgeBindings) {
        ObjectMapper om = new ObjectMapper();
        ObjectNode updatedNode = om.createObjectNode();
        ObjectNode knowledgeGraph = om.createObjectNode();
        JsonNode nodes = messageNode.get("message").get("knowledge_graph").get("nodes").deepCopy();
        ObjectNode edges = messageNode.get("message").get("knowledge_graph").get("edges").deepCopy();
        for (Iterator<Map.Entry<String, JsonNode>> it = newEdges.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> edge = it.next();
            edges.set(edge.getKey(), edge.getValue());
        }
        knowledgeGraph.set("nodes", nodes);
        knowledgeGraph.set("edges", edges);
        JsonNode queryGraph = messageNode.get("message").get("query_graph").deepCopy();
        updatedNode.set("query_graph", queryGraph);
        updatedNode.set("knowledge_graph", knowledgeGraph);
        if (newEdgeBindings.isEmpty()) {
            JsonNode results = messageNode.get("message").get("results").deepCopy();
            updatedNode.set("results", results);
        } else {
            // Note: making the dangerous assumption that there is exactly one object in the results array
            ArrayNode resultsArray = om.createArrayNode();
            ObjectNode results = om.createObjectNode();
            JsonNode nodeBindings = messageNode.get("message").get("results").get(0).get("node_bindings").deepCopy();
            ObjectNode edgeBindings = messageNode.get("message").get("results").get(0).get("edge_bindings").deepCopy();
            edgeBindings.set("", newEdgeBindings);
            results.set("node_bindings", nodeBindings);
            results.set("edge_bindings", edgeBindings);
            resultsArray.add(results);
            updatedNode.set("results", resultsArray);
        }
        return updatedNode;
    }

    private boolean isValid(JsonNode objectNode) {
        if (objectNode.hasNonNull("message")) {
            JsonNode messageNode = objectNode.get("message");
            if (messageNode.hasNonNull("query_graph") && messageNode.hasNonNull("knowledge_graph") && messageNode.hasNonNull("results")) {
                return messageNode.get("knowledge_graph").hasNonNull("nodes") &&
                        messageNode.get("knowledge_graph").hasNonNull("edges") &&
                        messageNode.get("results").get(0).hasNonNull("node_bindings") &&
                        messageNode.get("results").get(0).hasNonNull("edge_bindings");
            }
        }
        return false;
    }

    private List<List<String>> getAllConceptPairs(List<String> concepts) {
        HashSet<List<String>> uniquePairs = new HashSet<>();
        for (String concept1 : concepts) {
            for (String concept2 : concepts) {
                if (concept1.equals(concept2)) {
                    continue;
                }
                List<String> pair = Arrays.asList(concept1, concept2);
                pair.sort(Comparator.naturalOrder());
                uniquePairs.add(pair);
            }
        }
        return new ArrayList<>(uniquePairs);
    }
}
