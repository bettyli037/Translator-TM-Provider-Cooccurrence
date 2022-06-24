package edu.ucdenver.ccp.cooccurrence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.ValidationMessage;
import edu.ucdenver.ccp.cooccurrence.TRAPI.*;
import edu.ucdenver.ccp.cooccurrence.entities.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

@RestController
public class CooccurrenceController {

    private final NodeRepository nodeRepo;
    private final ObjectMapper objectMapper;
    private final LookupRepository lookupQueries;
    private final NodeNormalizerService sri;
    private static final List<String> documentParts = List.of("abstract", "title", "sentence");
    private static final Map<String, Integer> documentPartCounts = new HashMap<>();
    private Map<String, Integer> conceptCounts;
    public CooccurrenceController(NodeRepository repo, LookupRepository impl, NodeNormalizerService sri) {
        this.nodeRepo = repo;
        this.objectMapper = new ObjectMapper();
        this.lookupQueries = impl;
        this.sri = sri;
        conceptCounts = lookupQueries.getConceptCounts();
        for (String part : documentParts) {
            documentPartCounts.put(part, nodeRepo.getDocumentCount(part));
        }
    }

    // region: Endpoints
    @GetMapping("/refresh")
    public String getRefresh() {
        conceptCounts = lookupQueries.getConceptCounts();
        for (String part : documentParts) {
            documentPartCounts.put(part, nodeRepo.getDocumentCount(part));
        }
        return "Thanks!";
    }

    @GetMapping("/hierarchy/{curie}")
    public String getHierarchy(@PathVariable("curie") String curie) {
        Map<String, List<String>> hierarchy;
        if (curie.startsWith("biolink")) {
            List<String> topLevelCuries = lookupQueries.getCuriesForCategory(curie);
            hierarchy = lookupQueries.getDescendantHierarchy(topLevelCuries, new HashSet<>(topLevelCuries));
        } else {
            hierarchy = lookupQueries.getDescendantHierarchy(List.of(curie), Set.of(curie));
        }
        ObjectNode responseNode = objectMapper.createObjectNode();
        responseNode.set("hierarchy", objectMapper.convertValue(hierarchy, ObjectNode.class));
        Set<String> descendantsOnly = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : hierarchy.entrySet()) {
            descendantsOnly.addAll(entry.getValue());
        }
        responseNode.set("normalized_descendants", sri.getNormalizedNodes(new ArrayList<>(descendantsOnly)));
        return responseNode.toPrettyString();
    }

    @PostMapping("/query")
    public ResponseEntity<JsonNode> lookup(@RequestBody JsonNode requestNode) {
        if (!requestNode.hasNonNull("message")) {
            ObjectNode errorNode = objectMapper.createObjectNode();
            errorNode.put("error", "No message in request");
            return ResponseEntity.badRequest().body(errorNode);
        }
        JsonNode messageNode = requestNode.get("message");
        Validator validator = new Validator();
        Set<ValidationMessage> errors = validator.validateInput(messageNode.get("query_graph"));
        if (errors.size() > 0) {
            return ResponseEntity.unprocessableEntity().body(objectMapper.convertValue(errors, ArrayNode.class));
        }
        QueryGraph queryGraph = QueryGraph.parseJSON(messageNode.get("query_graph"));
        List<ConceptPair> conceptPairs = getConceptPairs(queryGraph); // This is the actual query portion.

        List<String> curies = conceptPairs.stream()
                .map(cp -> List.of(cp.getSubject(), cp.getObject()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
        JsonNode normalizedNodes = sri.getNormalizedNodes(curies);

        KnowledgeGraph knowledgeGraph = buildKnowledgeGraph(conceptPairs, normalizedNodes); // Equivalent to a fill operation.
        List<Result> resultsList = bindGraphs(queryGraph, knowledgeGraph); // Almost an atomic bind operation
        List<Result> completedResults = completeResults(resultsList); // Atomic complete_results operation

        // With all the information in place, we just pack it up into JSON to respond.
        ObjectNode responseNode = objectMapper.createObjectNode();
        responseNode.set("query_graph", queryGraph.toJSON());
        responseNode.set("knowledge_graph", knowledgeGraph.toJSON());
        List<JsonNode> jsonResults = completedResults.stream().map(Result::toJSON).collect(Collectors.toList());
        responseNode.set("results", objectMapper.convertValue(jsonResults, ArrayNode.class));
        return ResponseEntity.ok(responseNode);
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
    public ResponseEntity<JsonNode> overlay(@RequestBody String requestString) throws JsonProcessingException {
        JsonNode valueNode = objectMapper.readTree(requestString);
        JsonNode requestNode;
        // The SmartAPI status monitor sends a mangled requestString, which we have to "fix" by reading the first time.
        // After that we can use the value of that JsonNode to create the requestNode.
        if (valueNode.isValueNode()) {
            requestNode = objectMapper.readTree(valueNode.asText());
        } else { // If it parsed correctly the first time there's no need to do it again.
            requestNode = valueNode;
        }
        if (!isValid(requestNode)) {
            // TODO: figure out what caused it to fail validation and use it to build a ValidationError object
            return ResponseEntity.unprocessableEntity().body(objectMapper.createObjectNode().put("error", "Validation failed"));
        }
        JsonNode nodes = requestNode.get("message").get("knowledge_graph").get("nodes");
        ObjectNode newEdgesNode = objectMapper.createObjectNode();
        List<String> concepts = new ArrayList<>();
        ArrayNode newEdgeBindings = objectMapper.createArrayNode();
        for (Iterator<String> iter = nodes.fieldNames(); iter.hasNext(); ) {
            concepts.add(iter.next());
        }
        for (List<String> pair : getAllConceptPairs(concepts)) {
            String s = pair.get(0);
            String o = pair.get(1);
            List<String> documentParts = Arrays.asList("abstract", "title", "sentence");

            for (String part : documentParts) {
                Metrics cooccurrenceMetrics = getMetrics(s, o, part);
                if (!Double.isNaN(cooccurrenceMetrics.getNormalizedGoogleDistance())) {
                    String edgeID = s + "_" + o + "_" + part;
                    ObjectNode binding = objectMapper.createObjectNode();
                    binding.put("id", edgeID);
                    newEdgeBindings.add(binding);
                    JsonNode newEdge = createEdgeNode(edgeID, s, o, part, cooccurrenceMetrics);
                    newEdgesNode.set(edgeID, newEdge);
                }
            }
        }
        JsonNode responseNode = objectMapper.createObjectNode().set("message", updateMessageNode(requestNode, newEdgesNode, newEdgeBindings));
        return ResponseEntity.ok(responseNode);
    }

    // endregion

    // region: Operations

    // TODO: find a way to bind the graphs without relying on the QueryKey hints.
    private List<Result> bindGraphs(QueryGraph qGraph, KnowledgeGraph kGraph) {
        List<Result> results = new ArrayList<>();
        Map<String, QueryEdge> queryEdgeMap = qGraph.getEdges();
        Map<String, QueryNode> queryNodeMap = qGraph.getNodes();
        Map<String, KnowledgeNode> knowledgeNodeMap = kGraph.getNodes();
        for (Map.Entry<String, KnowledgeEdge> edgeEntry : kGraph.getEdges().entrySet()) {
            Result result = new Result();
            String queryGraphEdgeLabel = edgeEntry.getValue().getQueryKey();
            String knowledgeGraphEdgeLabel = edgeEntry.getKey();
            if (queryEdgeMap.containsKey(queryGraphEdgeLabel)) {
                result.addEdgeBinding(queryGraphEdgeLabel, knowledgeGraphEdgeLabel);
            }

            String knowledgeGraphSubjectLabel = edgeEntry.getValue().getSubject();
            KnowledgeNode subject = knowledgeNodeMap.get(knowledgeGraphSubjectLabel);
            String queryGraphSubjectLabel = subject.getQueryKey();
            if (queryNodeMap.containsKey(queryGraphSubjectLabel)) {
                result.addNodeBinding(queryGraphSubjectLabel, knowledgeGraphSubjectLabel);
            }

            String knowledgeGraphObjectLabel = edgeEntry.getValue().getObject();
            KnowledgeNode object = knowledgeNodeMap.get(knowledgeGraphObjectLabel);
            String queryGraphObjectLabel = object.getQueryKey();
            if (queryNodeMap.containsKey(queryGraphObjectLabel)) {
                result.addNodeBinding(queryGraphObjectLabel, knowledgeGraphObjectLabel);
            }
            results.add(result);
        }
        return results;
    }

    private List<Result> completeResults(List<Result> inputList) {
        List<Result> outputList = new ArrayList<>();
        for (int i = 0; i < inputList.size(); i++) {
            Result first = inputList.get(i);
            Map<String, Set<String>> firstEdges = first.getEdgeBindings();
            Map<String, Set<String>> firstNodes = first.getNodeBindings();
            List<Integer> mergeIndices = new ArrayList<>();
            for (int j = i + 1; j < inputList.size(); j++) {
                Result second = inputList.get(j);
                Map<String, Set<String>> secondEdges = second.getEdgeBindings();
                Map<String, Set<String>> secondNodes = second.getNodeBindings();
                if (firstEdges.keySet().equals(secondEdges.keySet())
                        && firstNodes.keySet().equals(secondNodes.keySet())) {
                    boolean isMergeable = true;
                    for (String nodeKey : firstNodes.keySet()) {
                        if (!firstNodes.get(nodeKey).equals(secondNodes.get(nodeKey))) {
                            isMergeable = false;
                            break;
                        }
                    }
                    if (isMergeable) {
                        mergeIndices.add(j);
                    }
                }
            }
            if (mergeIndices.size() > 0) {
                Map<String, Set<String>> nodeMap = new HashMap<>(firstNodes);
                Map<String, Set<String>> edgeMap = new HashMap<>(firstEdges);
                for (Integer index : mergeIndices) {
                    Map<String, Set<String>> secondEdges = inputList.get(index).getEdgeBindings();
                    for (String edgeKey : edgeMap.keySet()) {
                        edgeMap.get(edgeKey).addAll(secondEdges.get(edgeKey));
                    }
                }
                Result newResult = new Result(nodeMap, edgeMap);
                outputList.add(newResult);
            }
        }
        if (outputList.size() > 0) {
            return outputList;
        }
        return inputList;
    }

    // endregion

    // TODO: Use the TRAPI POJOs here instead of the hacked-together stuff currently in place
    //region Just Overlay Stuff

    private JsonNode createEdgeNode(String edgeID, String concept1, String concept2, String documentPart, Metrics cooccurrenceMetrics) {
        ObjectNode newEdge = objectMapper.createObjectNode();
        ObjectNode edgeAttributes = objectMapper.createObjectNode();
        newEdge.put("subject", concept1);
        newEdge.put("object", concept2);
        newEdge.put("predicate", "biolink:occurs_together_in_literature_with");
        edgeAttributes.put("attribute_type_id", "biolink:supporting_study_result");
        edgeAttributes.put("value", "tmkp:" + edgeID);
        edgeAttributes.put("value_type_id", "biolink:AbstractLevelConceptCooccurrenceAnalysisResult");
        String description;
        switch (documentPart) {
            case "sentence":
                description = "a single result from computing cooccurrence metrics between two concepts that cooccur at the sentence level";
                break;
            case "title":
                description = "a single result from computing cooccurrence metrics between two concepts that cooccur in the document title";
                break;
            case "abstract":
                description = "a single result from computing cooccurrence metrics between two concepts that cooccur in the abstract";
                break;
            default:
                description = "a single result from computing cooccurrence metrics between two concepts that cooccur in a document";
        }

        edgeAttributes.put("description", description);
        edgeAttributes.put("attribute_source", "infores:text-mining-provider-cooccurrence");
        edgeAttributes.set("attributes", cooccurrenceMetrics.toJSONArray());
        newEdge.set("attributes", edgeAttributes);
        return newEdge;
    }

    private Metrics getMetrics(String concept1, String concept2, String part) {
        int singleCount1, singleCount2, pairCount, totalConceptCount, totalDocumentCount;
        totalConceptCount = nodeRepo.getTotalConceptCount();
        totalDocumentCount = nodeRepo.getDocumentCount(part);
        Map<String, Map<String, Integer>> singleCountsMap = lookupQueries.getSingleCounts(List.of(concept1, concept2));
        singleCount1 = singleCountsMap.get(concept1).get(part);
        singleCount2 = singleCountsMap.get(concept2).get(part);
        Map<String, List<String>> cooccurrences = lookupQueries.getCooccurrences(Collections.singletonList(concept1), Collections.singletonList(concept2));
        pairCount = cooccurrences.get(concept1 + concept2 + part).size();
        return new Metrics(singleCount1, singleCount2, pairCount, totalConceptCount, totalDocumentCount, part);
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

    //endregion

    // region Lookup Stuff

    // TODO: look into what does/should happen if the same node name would be added by multiple different query graph sources.
    // Currently the last one would override the others in the KG, which would have implications for node binding (and probably other things).
    private KnowledgeGraph buildKnowledgeGraph(List<ConceptPair> conceptPairs, JsonNode normalizedNodes) {
        KnowledgeGraph kg = new KnowledgeGraph();
        for (ConceptPair pair : conceptPairs) {
            KnowledgeEdge edge = new KnowledgeEdge(pair.getSubject(), pair.getObject(), "biolink:occurs_together_in_literature_with", pair.getPairMetrics().toJSONArray());
            KnowledgeNode subjectNode = new KnowledgeNode(sri.getNodeName(pair.getSubject(), normalizedNodes), sri.getNodeCategories(pair.getSubject(), normalizedNodes));
            KnowledgeNode objectNode = new KnowledgeNode(sri.getNodeName(pair.getObject(), normalizedNodes), sri.getNodeCategories(pair.getObject(), normalizedNodes));

            // These are the only way I could think of to know which kedge and knode is connected to which qedge and qnode
            edge.setQueryKey(pair.getEdgeKey());
            subjectNode.setQueryKey(pair.getSubjectKey());
            objectNode.setQueryKey(pair.getObjectKey());

            kg.addEdge(pair.getSubject() + "_" + pair.getObject() + "_" + pair.getPart(), edge);
            kg.addNode(pair.getSubject(), subjectNode);
            kg.addNode(pair.getObject(), objectNode);
        }
        return kg;
    }

    private List<ConceptPair> getConceptPairs(QueryGraph graph) {
        List<ConceptPair> conceptPairs = new ArrayList<>();
        Map<String, QueryNode> nodeMap = graph.getNodes();
        Map<String, QueryEdge> edgeMap = graph.getEdges();
        for (Map.Entry<String, QueryEdge> edgeEntry : edgeMap.entrySet()) {
            String edgeKey = edgeEntry.getKey();
            conceptPairs.addAll(getConceptPairsForEdge(edgeKey, edgeEntry.getValue(), nodeMap));
        }
        return conceptPairs;
    }

    // The specifications allow for lists in place of either curie or category, but category list is
    // rather difficult to implement at the moment. Since the curies belonging to a category can overlap
    // with other categories, and the document lists can also overlap, getting the single count for a node
    // defined by a list of categories is not straightforward.
    private List<ConceptPair> getConceptPairsForEdge(String edgeKey, QueryEdge edge, Map<String, QueryNode> nodeMap) {
        List<ConceptPair> conceptPairs = new ArrayList<>();
        for (String predicate : edge.getPredicates()) {
            if (predicate.equals("biolink:occurs_together_in_literature_with") || predicate.isBlank()) {
                String subjectKey = edge.getSubject();
                String objectKey = edge.getObject();
                QueryNode subjectNode = nodeMap.get(subjectKey);
                QueryNode objectNode = nodeMap.get(objectKey);
                String subjectCategory = null, objectCategory = null;
                if (!subjectNode.getCategories().isEmpty()) {
                    subjectCategory = subjectNode.getCategories().get(0);
                }
                if (!objectNode.getCategories().isEmpty()) {
                    objectCategory = objectNode.getCategories().get(0);
                }
                List<ConceptPair> pairs = findConceptPairs(subjectNode.getIds(), subjectCategory, objectNode.getIds(), objectCategory);
                pairs.forEach(x -> x.setKeys(subjectKey, objectKey, edgeKey));
                conceptPairs.addAll(pairs);
            }
        }
        return conceptPairs;
    }

    // This method is necessarily complex because category based queries tend to have very large top level concept lists.
    // The query in getHierarchicalCounts goes very slowly with such large numbers, so we only use that method for concepts with descendants.
    // For the concepts with no descendants (which is the majority) we use the simpler and faster getSingleCounts query.
    private List<ConceptPair> findConceptPairs(List<String> subjectCurieList, String subjectCategory, List<String> objectCurieList, String objectCategory) {
        if ((subjectCurieList == null || subjectCurieList.isEmpty()) && (subjectCategory == null || subjectCategory.isBlank())
                && (objectCurieList == null || objectCurieList.isEmpty()) && (objectCategory == null || objectCategory.isBlank())) {
            return Collections.emptyList();
        }
        List<ConceptPair> conceptPairs = new ArrayList<>();
        boolean subjectCategoryQuery = subjectCurieList == null || subjectCurieList.isEmpty();
        boolean objectCategoryQuery = objectCurieList == null || objectCurieList.isEmpty();
        Map<String, Map<String, Integer>> topLevelSubjectCounts = new HashMap<>();
        Map<String, Map<String, Integer>> topLevelObjectCounts = new HashMap<>();
        Map<String, Map<String, BigInteger>> subjectHierarchyCounts, objectHierarchyCounts;
        List<String> subjectCuries = subjectCurieList;
        List<String> objectCuries = objectCurieList;
        if (subjectCategoryQuery) {
            topLevelSubjectCounts = lookupQueries.getSingleCounts(lookupQueries.getCuriesForCategory(subjectCategory));
            subjectCuries = lookupQueries.getCuriesForCategory(subjectCategory);
        }
        if (objectCategoryQuery) {
            topLevelObjectCounts = lookupQueries.getSingleCounts(lookupQueries.getCuriesForCategory(objectCategory));
            objectCuries = lookupQueries.getCuriesForCategory(objectCategory);
        }

        Map<String, List<String>> subjectHierarchy = lookupQueries.getDescendantHierarchy(subjectCuries, Collections.emptySet());
        Map<String, List<String>> objectHierarchy = lookupQueries.getDescendantHierarchy(objectCuries, Collections.emptySet());
        Map<String, Integer> pairCounts = getPairCounts(subjectHierarchy, objectHierarchy);

        if (subjectCategoryQuery) {
            Map<String, List<String>> subjectsWithDescendants = new HashMap<>();
            for (Map.Entry<String, List<String>> subjectEntry : subjectHierarchy.entrySet()) {
                if (subjectEntry.getValue().size() > 0) {
                    subjectsWithDescendants.put(subjectEntry.getKey(), subjectEntry.getValue());
                }
            }
            subjectHierarchyCounts = lookupQueries.getHierchicalCounts(subjectsWithDescendants, true);
        } else {
            subjectHierarchyCounts = lookupQueries.getHierchicalCounts(subjectHierarchy, true);
        }

        if (objectCategoryQuery) {
            Map<String, List<String>> objectsWithDescendants = new HashMap<>();
            for (Map.Entry<String, List<String>> objectEntry : objectHierarchy.entrySet()) {
                if (objectEntry.getValue().size() > 0) {
                    objectsWithDescendants.put(objectEntry.getKey(), objectEntry.getValue());
                }
            }
            objectHierarchyCounts = lookupQueries.getHierchicalCounts(objectsWithDescendants, true);
        } else {
            objectHierarchyCounts = lookupQueries.getHierchicalCounts(objectHierarchy, true);
        }

        for (String sub : subjectHierarchy.keySet()) {
            for (String obj : objectHierarchy.keySet()) {
                for (String part : documentParts) {
                    if (pairCounts.containsKey(sub + obj + part)) {
                        int pairCount = pairCounts.get(sub + obj + part);
                        ConceptPair pair = new ConceptPair(sub, obj, part, BigInteger.valueOf(pairCount));
                        int totalSubjectCount;
                        if (subjectHierarchyCounts.containsKey(sub) && subjectHierarchyCounts.get(sub).containsKey(part)) {
                            totalSubjectCount = subjectHierarchyCounts.get(sub).get(part).intValue();
                        } else {
                            totalSubjectCount = topLevelSubjectCounts.getOrDefault(sub, Collections.emptyMap()).getOrDefault(part, 0);
                        }
                        int totalObjectCount;
                        if (objectHierarchyCounts.containsKey(obj) && objectHierarchyCounts.get(obj).containsKey(part)) {
                            totalObjectCount = objectHierarchyCounts.get(obj).get(part).intValue();
                        } else {
                            totalObjectCount = topLevelObjectCounts.getOrDefault(obj, Collections.emptyMap()).getOrDefault(part, 0);
                        }
                        Metrics metrics = new Metrics(totalSubjectCount, totalObjectCount, pairCount, conceptCounts.get(part), documentPartCounts.get(part), part);
                        pair.setPairMetrics(metrics);
                        conceptPairs.add(pair);
                    }
                }
            }
        }
        return conceptPairs;
    }

    private Map<String, Integer> getPairCounts(Map<String, List<String>> subjectHierarchy, Map<String, List<String>> objectHierarchy) {
        Set<String> uniqueSubjects = subjectHierarchy.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        Set<String> uniqueObjects = objectHierarchy.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        uniqueSubjects.addAll(subjectHierarchy.keySet());
        uniqueObjects.addAll(objectHierarchy.keySet());
        Map<String, List<String>> cooccurrences = lookupQueries.getCooccurrencesByParts(new ArrayList<>(uniqueSubjects), new ArrayList<>(uniqueObjects));
        List<String> documentParts = List.of("abstract", "title", "sentence");
        // region: Create Dictionary for Pair Counts
        Map<String, Set<String>> documentDictionary = new HashMap<>();
        Set<String> completedKeys = new HashSet<>();
        for (Map.Entry<String, List<String>> subjectEntry : subjectHierarchy.entrySet()) {
            String parentSubject =  subjectEntry.getKey();
            for (Map.Entry<String, List<String>> objectEntry : objectHierarchy.entrySet()) {
                String parentObject = objectEntry.getKey();
                // look for cooccurrences between the parent subject and parent object
                for (String part : documentParts) {
                    String parentKey = parentSubject + parentObject + part;
                    if (cooccurrences.containsKey(parentKey) && !completedKeys.contains(parentKey)) {
                        completedKeys.add(parentKey);
                        if (documentDictionary.containsKey(parentKey)) {
                            documentDictionary.get(parentKey).addAll(cooccurrences.get(parentKey));
                        } else {
                            Set<String> docSet = new HashSet<>(cooccurrences.get(parentKey));
                            documentDictionary.put(parentKey, docSet);
                        }
                    }
                }
                // Look for cooccurrences between the parent subject and any child objects
                for (String childObject : objectEntry.getValue()) {
                    for (String part : documentParts) {
                        String parentKey = parentSubject + parentObject + part;
                        String childKey = parentSubject + childObject + part;
                        if (cooccurrences.containsKey(parentSubject + childObject + part) && !completedKeys.contains(parentKey + "_" + childKey)) {
                            completedKeys.add(parentKey + "_" + childKey);
                            if (documentDictionary.containsKey(parentKey)) {
                                documentDictionary.get(parentKey).addAll(cooccurrences.get(childKey));
                            } else {
                                Set<String> docSet = new HashSet<>(cooccurrences.get(childKey));
                                documentDictionary.put(parentKey, docSet);
                            }
                        }
                    }
                }
                // look for cooccurrences between the parent object and any child subjects
                for (String childSubject : subjectEntry.getValue()) {
                    for (String part : documentParts) {
                        String parentKey = parentSubject + parentObject + part;
                        String childKey = childSubject + parentObject + part;
                        if (cooccurrences.containsKey(childSubject + parentObject + part) && !completedKeys.contains(parentKey + "_" + childKey)) {
                            completedKeys.add(parentKey + "_" + childKey);
                            if (documentDictionary.containsKey(parentKey)) {
                                documentDictionary.get(parentKey).addAll(cooccurrences.get(childKey));
                            } else {
                                Set<String> docSet = new HashSet<>(cooccurrences.get(childKey));
                                documentDictionary.put(parentKey, docSet);
                            }
                        }
                    }
                    // Look for child-to-child cooccurrences.
                    for (String childObject : objectEntry.getValue()) {
                        for (String part : documentParts) {
                            String parentKey = parentSubject + parentObject + part;
                            String childKey = childSubject + childObject + part;
                            if (cooccurrences.containsKey(childKey) && !completedKeys.contains(parentKey + "_" + childKey)) {
                                completedKeys.add(parentKey + "_" + childKey);
                                if (documentDictionary.containsKey(parentKey)) {
                                    documentDictionary.get(parentKey).addAll(cooccurrences.get(childKey));
                                } else {
                                    Set<String> docSet = new HashSet<>(cooccurrences.get(childKey));
                                    documentDictionary.put(parentKey, docSet);
                                }
                            }
                        }
                    }
                }
            }
        }
        // endregion
        Map<String, Integer> pairCounts = new HashMap<>();
        for (Map.Entry<String, Set<String>> pairList : documentDictionary.entrySet()) {
            pairCounts.put(pairList.getKey(), pairList.getValue().size());
        }
        return pairCounts;
    }

    // endregion
}
