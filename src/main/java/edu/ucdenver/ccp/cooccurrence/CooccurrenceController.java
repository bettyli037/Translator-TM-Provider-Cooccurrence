package edu.ucdenver.ccp.cooccurrence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

@RestController
public class CooccurrenceController {

    private final NodeRepository nodeRepo;

    public CooccurrenceController(NodeRepository repo) {
        this.nodeRepo = repo;
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

    @PostMapping("/overlay")
    public String trapiTest(@RequestBody ObjectNode request) {
        if (!request.get("message").hasNonNull("knowledge_graph")) {
            return "Invalid Request";
        }
        JsonNode edges = request.get("message").get("knowledge_graph").get("edges");
        ObjectMapper om = new ObjectMapper();
        ObjectNode newEdgesNode = om.createObjectNode();
        for (Iterator<Map.Entry<String, JsonNode>> it = edges.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> edge = it.next();

            String s = edge.getValue().get("subject").asText();
            String o = edge.getValue().get("object").asText();
            List<String> concept1List = getAllAncestors(s);
            List<String> concept2List = getAllAncestors(o);
//            System.out.println(s + "_" + o);
//            concept1List.forEach(System.out::print);
//            System.out.println();
//            concept2List.forEach(System.out::print);
//            System.out.println();
            Metrics cooccurrenceMetrics = getMetrics(concept1List, concept2List, "abstract");
            if (Double.isNaN(cooccurrenceMetrics.getNormalizedGoogleDistance())) {
//                System.out.println("Nothing to see here");
                continue;
            }
            ObjectNode newEdge = om.createObjectNode();
            newEdge.put("subject", s);
            newEdge.put("object", o);
            newEdge.put("predicate", "biolink:occurs_together_in_literature_with");
            ObjectNode attributesNode = om.createObjectNode();
            attributesNode.put("type", "cooccurrence");
            ObjectNode valueNode = cooccurrenceMetrics.toJSON();
            valueNode.put("predicate", "biolink:occurs_together_in_literature_with");
            valueNode.put("provided_by", "cooccurrence");
            attributesNode.set("value", valueNode);
            newEdge.set("attributes", attributesNode);
            newEdgesNode.set(s + "_" + o + "_abstract", newEdge);
        }
        JsonNode responseNode = om.createObjectNode().set("message", updateMessageNode(request, newEdgesNode));
        return responseNode.toPrettyString();
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

    private JsonNode updateMessageNode(JsonNode messageNode, JsonNode newEdges) {
        ObjectNode updatedNode = (new ObjectMapper()).createObjectNode();
        ObjectNode knowledgeGraph = (new ObjectMapper()).createObjectNode();
        JsonNode queryGraph = messageNode.get("message").get("query_graph").deepCopy();
        JsonNode results = messageNode.get("message").get("results").deepCopy();
        JsonNode nodes = messageNode.get("message").get("knowledge_graph").get("nodes").deepCopy();
        ObjectNode edges = messageNode.get("message").get("knowledge_graph").get("edges").deepCopy();
        for (Iterator<Map.Entry<String, JsonNode>> it = newEdges.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> edge = it.next();
            edges.set(edge.getKey(), edge.getValue());
        }
        knowledgeGraph.set("nodes", nodes);
        knowledgeGraph.set("edges", edges);
        updatedNode.set("query_graph", queryGraph);
        updatedNode.set("knowledge_graph", knowledgeGraph);
        updatedNode.set("results", results);
        return updatedNode;
    }
}
