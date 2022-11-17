package edu.ucdenver.ccp.cooccurrence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.ValidationMessage;
import edu.ucdenver.ccp.cooccurrence.TRAPI.*;
import edu.ucdenver.ccp.cooccurrence.entities.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

@RestController
public class CooccurrenceController {

    public static Logger logger = LoggerFactory.getLogger(CooccurrenceController.class);

    private final NodeRepository nodeRepo;
    private final ObjectMapper objectMapper;
    private final LookupRepository lookupQueries;
    public static final List<String> documentParts = List.of("abstract", "title", "sentence", "article");
    private static final Map<String, Integer> documentPartCounts = new HashMap<>();

    private static final List<String> supportedPredicates = List.of("biolink:related_to", "biolink:related_to_at_instance_level", "biolink:associated_with",
            "biolink:correlated_with", "biolink:occurs_together_in_literature_with");
    private Map<String, Integer> conceptCounts;

    public CooccurrenceController(NodeRepository repo, LookupRepository impl) {
        this.nodeRepo = repo;
        this.objectMapper = new ObjectMapper();
        this.lookupQueries = impl;
        conceptCounts = lookupQueries.getConceptCounts();
        for (String part : documentParts) {
            documentPartCounts.put(part, nodeRepo.getDocumentCount(part));
        }
    }

    // region: Endpoints
    @GetMapping("/refresh")
    public JsonNode getRefresh() {
        conceptCounts = lookupQueries.getConceptCounts();
        for (String part : documentParts) {
            documentPartCounts.put(part, nodeRepo.getDocumentCount(part));
        }
        ObjectNode responseNode = objectMapper.createObjectNode();
        responseNode.set("concepts", objectMapper.valueToTree(conceptCounts));
        responseNode.set("documents", objectMapper.valueToTree(documentPartCounts));
        return responseNode;
    }

    @PostMapping("/query")
    public ResponseEntity<JsonNode> lookup(@RequestBody JsonNode requestNode) {
        long startTime = System.currentTimeMillis();
        if (!requestNode.hasNonNull("message")) {
            ObjectNode errorNode = objectMapper.createObjectNode();
            errorNode.put("error", "No message in request");
            logger.warn("Lookup request with no message");
            return ResponseEntity.badRequest().body(errorNode);
        }
        JsonNode messageNode = requestNode.get("message");
        Validator validator = new Validator();
        Set<ValidationMessage> errors = validator.validateInput(messageNode.get("query_graph"));
        if (errors.size() > 0) {
            logger.warn("Lookup message failed validation");
            logger.debug(StringUtils.join(errors, "|"));
            return ResponseEntity.unprocessableEntity().body(objectMapper.convertValue(errors, ArrayNode.class));
        }
        QueryGraph queryGraph = QueryGraph.parseJSON(messageNode.get("query_graph"));

        List<AttributeConstraint> unsupportedConstraints = new ArrayList<>();
        for (Map.Entry<String, QueryEdge> edgeEntry : queryGraph.getEdges().entrySet()) {
            unsupportedConstraints.addAll(edgeEntry.getValue().getAttributeConstraints().stream().filter(x -> !x.isSupported()).collect(Collectors.toList()));
        }
        if (unsupportedConstraints.size() > 0) {
            ObjectNode errorNode = objectMapper.createObjectNode();
            errorNode.put("errorCode", "UnsupportedConstraint");
            errorNode.set("constraints", objectMapper.convertValue(unsupportedConstraints.stream().map(AttributeConstraint::getId).collect(Collectors.toList()), ArrayNode.class));
            return ResponseEntity.badRequest().body(errorNode);
        }

        logger.info(String.format("Starting lookup with %d edges and %d nodes", queryGraph.getEdges().size(), queryGraph.getNodes().size()));
        List<ConceptPair> initialPairs = getConceptPairs(queryGraph); // This is the actual query portion.
        Map<String, QueryEdge> edgeMap = queryGraph.getEdges();
        List<ConceptPair> conceptPairs = new ArrayList<>();
        for (ConceptPair pair : initialPairs) {
            QueryEdge edge = edgeMap.get(pair.getEdgeKey());
            if (edge.getAttributeConstraints().size() == 0 || pair.meetsConstraints(edge.getAttributeConstraints())) {
                conceptPairs.add(pair);
            }
        }
        List<String> curies = conceptPairs.stream()
                .map(cp -> List.of(cp.getSubject(), cp.getObject()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
//        JsonNode normalizedNodes = sri.getNormalizedNodes(curies);
        Map<String, List<String>> categoryMap = lookupQueries.getCategoriesForCuries(curies);
        Map<String, String> labelMap = lookupQueries.getLabels(curies);

        KnowledgeGraph knowledgeGraph = buildKnowledgeGraph(conceptPairs, labelMap, categoryMap); // Equivalent to a fill operation.
        List<Result> resultsList = bindGraphs(queryGraph, knowledgeGraph); // Almost an atomic bind operation
        List<Result> completedResults = completeResults(resultsList); // Atomic complete_results operation

        // With all the information in place, we just pack it up into JSON to respond.
        ObjectNode responseMessageNode = objectMapper.createObjectNode();
        responseMessageNode.set("query_graph", queryGraph.toJSON());
        responseMessageNode.set("knowledge_graph", knowledgeGraph.toJSON());
        List<JsonNode> jsonResults = completedResults.stream().map(Result::toJSON).collect(Collectors.toList());
        responseMessageNode.set("results", objectMapper.convertValue(jsonResults, ArrayNode.class));
        logger.info("Lookup completed in " + (System.currentTimeMillis() - startTime) + "ms");
        ObjectNode responseNode = objectMapper.createObjectNode();
        responseNode.set("message", responseMessageNode);
        return ResponseEntity.ok(responseNode);
    }


    @GetMapping("/meta_knowledge_graph")
    public JsonNode getMetaKnowledgeGraph() {
        long startTime = System.currentTimeMillis();
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
        logger.info("MKG completed in " + (System.currentTimeMillis() - startTime) + "ms");
        return responseNode;
    }

    @PostMapping("/overlay")
    public ResponseEntity<JsonNode> overlay(@RequestBody String requestString) throws JsonProcessingException {
        long startTime = System.currentTimeMillis();
        JsonNode valueNode = objectMapper.readTree(requestString);
        JsonNode requestNode;
        // The SmartAPI status monitor sends a mangled requestString, which we have to "fix" by reading the first time.
        // After that we can use the value of that JsonNode to create the requestNode.
        if (valueNode.isValueNode()) {
            requestNode = objectMapper.readTree(valueNode.asText());
        } else { // If it parsed correctly the first time there's no need to do it again.
            requestNode = valueNode;
        }
        JsonNode messageNode = requestNode.get("message");
        Validator validator = new Validator();
        Set<ValidationMessage> errors = validator.validateInput(messageNode.get("query_graph"));
        if (errors.size() > 0) {
            return ResponseEntity.unprocessableEntity().body(objectMapper.convertValue(errors, ArrayNode.class));
        }
        errors = validator.validateInput(messageNode.get("knowledge_graph"));
        if (errors.size() > 0) {
            return ResponseEntity.unprocessableEntity().body(objectMapper.convertValue(errors, ArrayNode.class));
        }
        JsonNode nodes = messageNode.get("knowledge_graph").get("nodes");
        ObjectNode newEdgesNode = objectMapper.createObjectNode();
        List<String> concepts = new ArrayList<>();
        ArrayNode newEdgeBindings = objectMapper.createArrayNode();
        for (Iterator<String> iter = nodes.fieldNames(); iter.hasNext(); ) {
            concepts.add(iter.next());
        }
        for (List<String> pair : getAllConceptPairs(concepts)) {
            String s = pair.get(0);
            String o = pair.get(1);

            for (String part : documentParts) {
                Metrics cooccurrenceMetrics = getMetrics(s, o, part);
                if (cooccurrenceMetrics.getPairCount() != 0 && !Double.isNaN(cooccurrenceMetrics.getNormalizedGoogleDistance())) {
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
        logger.info("Overlay completed in " + (System.currentTimeMillis() - startTime) + "ms");
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
        singleCount1 = singleCountsMap.getOrDefault(concept1, Collections.emptyMap()).getOrDefault(part, 0);
        singleCount2 = singleCountsMap.getOrDefault(concept2, Collections.emptyMap()).getOrDefault(part, 0);
        // TODO: Make a single concept pair version of getCooccurrences
        Map<String, List<String>> cooccurrences = lookupQueries.getPairCounts(Collections.singletonList(concept1), Collections.singletonList(concept2));
        pairCount = cooccurrences.getOrDefault(concept1 + concept2 + part, Collections.emptyList()).size();
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
    private KnowledgeGraph buildKnowledgeGraph(List<ConceptPair> conceptPairs, Map<String, String> labelMap, Map<String, List<String>> categoryMap) {
        KnowledgeGraph kg = new KnowledgeGraph();
        for (ConceptPair pair : conceptPairs) {
            KnowledgeEdge edge = new KnowledgeEdge(pair.getSubject(), pair.getObject(), "biolink:occurs_together_in_literature_with", pair.getPairMetrics().toAttributeList());
            KnowledgeNode subjectNode = new KnowledgeNode(
                    labelMap.getOrDefault(pair.getSubject(), ""),
                    categoryMap.getOrDefault(pair.getSubject(), Collections.emptyList()));
            KnowledgeNode objectNode = new KnowledgeNode(
                    labelMap.getOrDefault(pair.getObject(), ""),
                    categoryMap.getOrDefault(pair.getObject(), Collections.emptyList()));

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
            if (supportedPredicates.contains(predicate) || predicate.isBlank()) {
                String subjectKey = edge.getSubject();
                String objectKey = edge.getObject();
                //TODO: confirm the subject and object keys from the edge exist in the nodeMap
                QueryNode subjectNode = nodeMap.get(subjectKey);
                QueryNode objectNode = nodeMap.get(objectKey);
                String subjectCategory = null, objectCategory = null;
                if (!subjectNode.getCategories().isEmpty()) {
                    subjectCategory = subjectNode.getCategories().get(0); // Getting only the top category on the assumption that it's the most specific one.
                }
                if (!objectNode.getCategories().isEmpty()) {
                    objectCategory = objectNode.getCategories().get(0); // Getting only the top category on the assumption that it's the most specific one.
                }
                logger.debug(String.format("Lookup: (%s, %s, %s)\n", subjectNode.toJSON().toString(), predicate, objectNode.toJSON().toString()));
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
        long t0 = System.currentTimeMillis();
        if ((subjectCurieList == null || subjectCurieList.isEmpty()) && (subjectCategory == null || subjectCategory.isBlank())
                && (objectCurieList == null || objectCurieList.isEmpty()) && (objectCategory == null || objectCategory.isBlank())) {
            return Collections.emptyList();
        }
        List<ConceptPair> conceptPairs = new ArrayList<>();
        boolean subjectCategoryQuery = subjectCurieList == null || subjectCurieList.isEmpty();
        boolean objectCategoryQuery = objectCurieList == null || objectCurieList.isEmpty();
        Map<String, Map<String, Integer>> subjectHierarchyCounts, objectHierarchyCounts;
        List<String> subjectCuries = subjectCurieList;
        List<String> objectCuries = objectCurieList;
        if (subjectCategoryQuery) {
            subjectCuries = lookupQueries.getCuriesForCategory(subjectCategory);
        }
        if (objectCategoryQuery) {
            objectCuries = lookupQueries.getCuriesForCategory(objectCategory);
        }

        long t1 = System.currentTimeMillis();
        logger.debug("Curies retrieved in " + (t1 - t0) + "ms");

        Map<String, Map<String, Integer>> topLevelSubjectCounts = lookupQueries.getSingleCounts(subjectCuries);
        Map<String, Map<String, Integer>> topLevelObjectCounts = lookupQueries.getSingleCounts(objectCuries);

        long t2 = System.currentTimeMillis();
        logger.debug("Single counts retrieved in " + (t2 - t1) + "ms");

        Map<String, List<String>> subjectHierarchy = lookupQueries.getDescendantHierarchy(subjectCuries);
        Map<String, List<String>> objectHierarchy = lookupQueries.getDescendantHierarchy(objectCuries);

        long t3 = System.currentTimeMillis();
        logger.debug("Hierarchies retrieved in " + (t3 - t2) + "ms");

        Map<String, List<String>> cooccurrences = getPairCounts(subjectHierarchy, objectHierarchy);
        Map<String, Integer> pairCounts = new HashMap<>();
        for (Map.Entry<String, List<String>> pairList : cooccurrences.entrySet()) {
            pairCounts.put(pairList.getKey(), pairList.getValue().size());
        }

        long t4 = System.currentTimeMillis();
        logger.debug("Pair counts retrieved in " + (t4 - t3) + "ms");

        subjectHierarchyCounts = lookupQueries.getHierchicalCounts(subjectCuries);
        objectHierarchyCounts = lookupQueries.getHierchicalCounts(objectCuries);

        long t5 = System.currentTimeMillis();
        logger.debug("Hierarchical counts retrieved in " + (t5 - t4) + "ms");
        for (String sub : subjectCuries) {
            for (String obj : objectCuries) {
                for (String part : documentParts) {
                    if (pairCounts.containsKey(sub + obj + part)) {
                        int pairCount = pairCounts.get(sub + obj + part);
                        ConceptPair pair = new ConceptPair(sub, obj, part, BigInteger.valueOf(pairCount));
                        int totalSubjectCount;
                        if (subjectHierarchyCounts.containsKey(sub) &&
                                subjectHierarchyCounts.get(sub).containsKey(part) &&
                                subjectHierarchyCounts.get(sub).get(part) > 0) {
                            totalSubjectCount = subjectHierarchyCounts.get(sub).get(part);
                        } else {
                            totalSubjectCount = topLevelSubjectCounts.getOrDefault(sub, Collections.emptyMap()).getOrDefault(part, 0);
                        }
                        int totalObjectCount;
                        // NB: this implies that if a curie exists in the hierarchy counts but not for that part it should be treated as a single count.
                        // I'm not totally sure that's true though. I will need to give that a think later. -Edgar
                        if (objectHierarchyCounts.containsKey(obj) &&
                                objectHierarchyCounts.get(obj).containsKey(part) &&
                                objectHierarchyCounts.get(obj).get(part) > 0) {
                            totalObjectCount = objectHierarchyCounts.get(obj).get(part);
                        } else {
                            totalObjectCount = topLevelObjectCounts.getOrDefault(obj, Collections.emptyMap()).getOrDefault(part, 0);
                        }
                        if (totalSubjectCount == 0 || totalObjectCount == 0) {
                            continue;
                        }
                        Metrics metrics = new Metrics(totalSubjectCount, totalObjectCount, pairCount, conceptCounts.getOrDefault(part, 0),
                                documentPartCounts.getOrDefault(part, 0), part);
                        metrics.setDocumentIdList(cooccurrences.get(sub + obj + part).stream().map(hash -> hash.split("_")[0]).collect(Collectors.toList()));
                        pair.setPairMetrics(metrics);
                        conceptPairs.add(pair);
                    }
                }
            }
        }
        return conceptPairs;
    }

    private Map<String, List<String>> getPairCounts(Map<String, List<String>> subjectHierarchy, Map<String, List<String>> objectHierarchy) {
//        long t1 = System.currentTimeMillis();
        Map<String, List<String>> cooccurrences = lookupQueries.getCooccurrencesByParts(new ArrayList<>(subjectHierarchy.keySet()), new ArrayList<>(objectHierarchy.keySet()));
//        long t2 = System.currentTimeMillis();
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
//        Map<String, Integer> pairCounts = new HashMap<>();
//        for (Map.Entry<String, Set<String>> pairList : documentDictionary.entrySet()) {
//            pairCounts.put(pairList.getKey(), pairList.getValue().size());
//        }
//        long t3 = System.currentTimeMillis();
//        System.out.format("Time to get cooccurrences: %d\tTime to create dictionary: %d\n", t2 - t1, t3 - t2);
        return cooccurrences;
    }

    // endregion
}
