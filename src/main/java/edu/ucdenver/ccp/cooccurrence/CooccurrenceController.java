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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@RestController
public class CooccurrenceController {

    public static Logger logger = LoggerFactory.getLogger(CooccurrenceController.class);

    @Autowired
    private CacheManager cacheManager;
    private final NodeRepository nodeRepo;
    private final ObjectMapper objectMapper;
    private final LookupRepository lookupQueries;
    private final NodeNormalizerService sri;
    public static final List<String> documentParts = List.of("abstract", "title", "sentence", "article");
    private static final Map<String, Integer> documentPartCounts = new HashMap<>();

    private static final List<String> supportedPredicates = List.of("biolink:related_to", "biolink:related_to_at_instance_level", "biolink:associated_with",
            "biolink:correlated_with", "biolink:occurs_together_in_literature_with");
    private Map<String, Integer> conceptCounts;
    private List<String> invalidClasses;

    private final int NN_BATCH_SIZE = 1000;

    public CooccurrenceController(NodeRepository repo, LookupRepository impl, NodeNormalizerService sri) {
        this.nodeRepo = repo;
        this.objectMapper = new ObjectMapper();
        this.lookupQueries = impl;
        this.sri = sri;
        conceptCounts = lookupQueries.getConceptCounts();
        for (String part : documentParts) {
            documentPartCounts.put(part, nodeRepo.getDocumentCount(part));
        }
        invalidClasses = BiolinkService.getClasses(BiolinkService.getBiolinkNode());
    }

    // region: Endpoints

    @GetMapping("/version")
    public JsonNode getVersion() {
        ObjectNode responseNode = objectMapper.createObjectNode();
        responseNode.put("version", "0.3.0");
        return responseNode;
    }

    @GetMapping("/refresh")
    public JsonNode getRefresh() {
        invalidClasses = BiolinkService.getClasses(BiolinkService.getBiolinkNode());
        conceptCounts = lookupQueries.getConceptCounts();
        for (String part : documentParts) {
            documentPartCounts.put(part, nodeRepo.getDocumentCount(part));
        }
        ObjectNode responseNode = objectMapper.createObjectNode();
        responseNode.set("concepts", objectMapper.valueToTree(conceptCounts));
        responseNode.set("documents", objectMapper.valueToTree(documentPartCounts));
        return responseNode;
    }

    @GetMapping("/cache")
    public JsonNode getCacheSize() {
        Collection<String> names = cacheManager.getCacheNames();
        HashMap<String, Integer> cacheSizes = new HashMap<>(names.size());
        for (String name : names) {
            Cache cache = cacheManager.getCache(name);
            if (cache != null) {
                ConcurrentMap<Object, Object> map = (ConcurrentMap<Object, Object>) cache.getNativeCache();
                StringBuilder builder = new StringBuilder();
                for (Map.Entry<Object, Object> entry : map.entrySet()) {
                    builder.append(entry.getKey());
                    builder.append(':');
                    builder.append(entry.getValue());
                    builder.append('\n');
                }
                logger.debug(name + " ::: " + builder.toString().length());
                cacheSizes.put(name, builder.toString().length());
            }
        }
        return objectMapper.valueToTree(cacheSizes);
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
        // TRAPI specs allow several named properties as well as additional properties, but we only care about "message" so the rest just get passed along.
        // TODO: make use of the log_level property
        Map<String, JsonNode> otherAttributes = new HashMap<>();
        Iterator<String> keyIterator = requestNode.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("message")) {
                continue;
            }
            otherAttributes.put(key, requestNode.get(key));
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
        // Remove partial results that do not satisfy attribute constraints, if any
        for (ConceptPair pair : initialPairs) {
            QueryEdge edge = edgeMap.get(pair.getEdgeKey());
            if (edge.getAttributeConstraints().size() > 0) {
                ConceptPair constrainedConceptPair = pair;
                for (AttributeConstraint constraint : edge.getAttributeConstraints()) {
                    constrainedConceptPair = constrainedConceptPair.satisfyConstraint(constraint);
                    if (constrainedConceptPair == null) {
                        break;
                    }
                }
                if (constrainedConceptPair != null) {
                    conceptPairs.add(constrainedConceptPair);
                }
            } else {
                conceptPairs.add(pair);
            }
        }
        List<String> curies = conceptPairs.stream()
                .map(cp -> List.of(cp.getSubject(), cp.getObject()))
                .flatMap(List::stream)
                .collect(Collectors.toList());
        JsonNode normalizedNodes = sri.getNormalizedNodesInBatches(curies, NN_BATCH_SIZE);
        Map<String, List<String>> categoryMap = lookupQueries.getCategoriesForCuries(curies);
        Map<String, String> labelMap = lookupQueries.getLabels(curies);

        KnowledgeGraph knowledgeGraph = buildKnowledgeGraph(conceptPairs, labelMap, categoryMap, normalizedNodes); // Equivalent to a fill operation.
        List<Result> resultsList = bindGraphs(queryGraph, knowledgeGraph); // Almost an atomic bind operation
//        List<Result> completedResults = completeResults(resultsList); // Atomic complete_results operation

        // With all the information in place, we just pack it up into JSON to respond.
        ObjectNode responseMessageNode = objectMapper.createObjectNode();
        responseMessageNode.set("query_graph", queryGraph.toJSON());
        responseMessageNode.set("knowledge_graph", knowledgeGraph.toJSON());
        List<JsonNode> jsonResults = resultsList.stream().map(Result::toJSON).collect(Collectors.toList());
        responseMessageNode.set("results", objectMapper.convertValue(jsonResults, ArrayNode.class));
        logger.info("Lookup completed in " + (System.currentTimeMillis() - startTime) + "ms");
        ObjectNode responseNode = objectMapper.createObjectNode();
        responseNode.set("message", responseMessageNode);
        for (Map.Entry<String, JsonNode> attribute : otherAttributes.entrySet()) {
            responseNode.set(attribute.getKey(), attribute.getValue());
        }
        return ResponseEntity.ok(responseNode);
    }


    // TODO: add a TRAPI class for the MetaKnowledgeGraph and associated subclasses
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

        ArrayNode edgeMetaAttributeArray = objectMapper.createArrayNode();

        ObjectNode partNode = objectMapper.createObjectNode();
        partNode.put("attribute_type_id", "biolink:supporting_text_located_in");
        partNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");
        partNode.put("constraint_use", true);
        partNode.put("constraint_name", "biolink:supporting_text_located_in");
        edgeMetaAttributeArray.add(partNode);

        ObjectNode subjectNode = objectMapper.createObjectNode();
        subjectNode.put("attribute_type_id", "biolink:concept_count_subject");
        subjectNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");
        subjectNode.put("constraint_use", true);
        subjectNode.put("constraint_name", "biolink:concept_count_subject");
        edgeMetaAttributeArray.add(subjectNode);

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("attribute_type_id", "biolink:concept_count_object");
        objectNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");
        objectNode.put("constraint_use", true);
        objectNode.put("constraint_name", "biolink:concept_count_object");
        edgeMetaAttributeArray.add(objectNode);

        ObjectNode pairNode = objectMapper.createObjectNode();
        pairNode.put("attribute_type_id", "biolink:concept_pair_count");
        pairNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");
        pairNode.put("constraint_use", true);
        pairNode.put("constraint_name", "biolink:concept_pair_count");
        edgeMetaAttributeArray.add(pairNode);

        ObjectNode ngdNode = objectMapper.createObjectNode();
        ngdNode.put("attribute_type_id", "biolink:tmkp_normalized_google_distance");
        ngdNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");
        ngdNode.put("constraint_use", true);
        ngdNode.put("constraint_name", "biolink:tmkp_normalized_google_distance");
        edgeMetaAttributeArray.add(ngdNode);

        ObjectNode npmiMaxNode = objectMapper.createObjectNode();
        npmiMaxNode.put("attribute_type_id", "biolink:tmkp_normalized_pointwise_mutual_information_max");
        npmiMaxNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");
        npmiMaxNode.put("constraint_use", true);
        npmiMaxNode.put("constraint_name", "biolink:tmkp_normalized_pointwise_mutual_information_max");
        edgeMetaAttributeArray.add(npmiMaxNode);

        ObjectNode mdNode = objectMapper.createObjectNode();
        mdNode.put("attribute_type_id", "biolink:tmkp_mutual_dependence");
        mdNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");
        mdNode.put("constraint_use", true);
        mdNode.put("constraint_name", "biolink:tmkp_mutual_dependence");
        edgeMetaAttributeArray.add(mdNode);

        ObjectNode lfbmdNode = objectMapper.createObjectNode();
        lfbmdNode.put("attribute_type_id", "biolink:tmkp_log_frequency_biased_mutual_dependence");
        lfbmdNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");
        lfbmdNode.put("constraint_use", true);
        lfbmdNode.put("constraint_name", "biolink:tmkp_log_frequency_biased_mutual_dependence");
        edgeMetaAttributeArray.add(lfbmdNode);

        ArrayNode edgeMetadataArray = objectMapper.createArrayNode();
        for (EdgeMetadata em : edgeMetadata) {
            ObjectNode entry = objectMapper.createObjectNode();
            entry.put("subject", em.getSubject());
            entry.put("object", em.getObject());
            entry.put("predicate", em.getPredicate());
            entry.set("knowledge_types", objectMapper.nullNode());
            entry.set("attributes", edgeMetaAttributeArray);
            entry.set("qualifiers", objectMapper.nullNode());
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
//        Set<ValidationMessage> errors = validator.validateMessage(messageNode);
        if (errors.size() > 0) {
            return ResponseEntity.unprocessableEntity().body(objectMapper.convertValue(errors, ArrayNode.class));
        }
        errors = validator.validateInput(messageNode.get("knowledge_graph"));
        if (errors.size() > 0) {
            return ResponseEntity.unprocessableEntity().body(objectMapper.convertValue(errors, ArrayNode.class));
        }

        Map<String, JsonNode> otherAttributes = new HashMap<>();
        Iterator<String> keyIterator = requestNode.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("message")) {
                continue;
            }
            otherAttributes.put(key, requestNode.get(key));
        }

        KnowledgeGraph knowledgeGraph = KnowledgeGraph.parseJSON(messageNode.get("knowledge_graph"));
        List<Result> results = new ArrayList<>();
        Iterator<JsonNode> resultsIterator = messageNode.get("results").elements();
        while (resultsIterator.hasNext()) {
            Result result = Result.parseJSON(resultsIterator.next());
            if (result != null) {
                results.add(result);
            }
        }
        List<String> curies = new ArrayList<>(knowledgeGraph.getNodes().keySet());

        for (List<String> pair : getAllConceptPairs(curies)) {
            String s = pair.get(0);
            String o = pair.get(1);
            logger.debug("Checking concept pair (" + s + ", " + o + ")");
            ConceptPair conceptPair = new ConceptPair(s, o);
            boolean hasMetrics = false;
            for (String part : documentParts) {
                Metrics cooccurrenceMetrics = getMetrics(s, o, part);
                if (cooccurrenceMetrics.getPairCount() != 0 && !Double.isNaN(cooccurrenceMetrics.getNormalizedGoogleDistance())) {
                    conceptPair.setPairMetrics(part, cooccurrenceMetrics);
                    hasMetrics = true;
                    logger.debug("Metrics added");
                }
            }
            if (!hasMetrics) {
                continue;
            }
            String edgeId = s + "_" + o;
            KnowledgeEdge edge = new KnowledgeEdge(conceptPair.getSubject(), conceptPair.getObject(),
                    "biolink:occurs_together_in_literature_with", conceptPair.toAttributeList());
            RetrievalSource source = new RetrievalSource();
            source.setResource("infores:text-mining-provider-cooccurrence");
            source.setResourceRole("primary_knowledge_source");
            edge.addSource(source);
            knowledgeGraph.addEdge(edgeId, edge);
            logger.debug("Added edge " + edgeId);
            for (Result result : results) {
                if (result.bindsNodeCurie(s) && result.bindsNodeCurie(o)) {
                    logger.debug("Result binds (" + s + ", " + o + ")");
                    Analysis analysis = new Analysis();
                    analysis.setResourceId("infores:text-mining-provider-cooccurrence");
                    EdgeBinding edgeBinding = new EdgeBinding();
                    edgeBinding.setId(edgeId);
                    analysis.addEdgeBinding("", edgeBinding);
                    result.addAnalysis(analysis);
                }
            }
        }
        ObjectNode responseMessageNode = objectMapper.createObjectNode();
        responseMessageNode.set("query_graph", messageNode.get("query_graph"));
        responseMessageNode.set("knowledge_graph", knowledgeGraph.toJSON());
        ArrayNode resultsNode = objectMapper.createArrayNode();
        for (Result result : results) {
            resultsNode.add(result.toJSON());
        }
        responseMessageNode.set("results", resultsNode);
        ObjectNode responseNode = objectMapper.createObjectNode().set("message", responseMessageNode);
        for (Map.Entry<String, JsonNode> attribute : otherAttributes.entrySet()) {
            responseNode.set(attribute.getKey(), attribute.getValue());
        }
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
            Analysis analysis = new Analysis();
            analysis.setResourceId("infores:text-mining-provider-cooccurrence");
            String queryGraphEdgeLabel = edgeEntry.getValue().getQueryKey();
            String knowledgeGraphEdgeLabel = edgeEntry.getKey();
            EdgeBinding edgeBinding = new EdgeBinding();
            edgeBinding.setId(knowledgeGraphEdgeLabel);
            if (queryEdgeMap.containsKey(queryGraphEdgeLabel)) {
                analysis.addEdgeBinding(queryGraphEdgeLabel, edgeBinding);
                result.addAnalysis(analysis);
            }

            String knowledgeGraphSubjectLabel = edgeEntry.getValue().getSubject();
            KnowledgeNode subject = knowledgeNodeMap.get(knowledgeGraphSubjectLabel);
            String queryGraphSubjectLabel = subject.getQueryKey();
            NodeBinding subjectNodeBinding = new NodeBinding();
            subjectNodeBinding.setId(knowledgeGraphSubjectLabel);
            if (queryNodeMap.containsKey(queryGraphSubjectLabel)) {
                result.addNodeBinding(queryGraphSubjectLabel, subjectNodeBinding);
            }

            String knowledgeGraphObjectLabel = edgeEntry.getValue().getObject();
            KnowledgeNode object = knowledgeNodeMap.get(knowledgeGraphObjectLabel);
            String queryGraphObjectLabel = object.getQueryKey();
            NodeBinding objectNodeBinding = new NodeBinding();
            objectNodeBinding.setId(knowledgeGraphObjectLabel);
            if (queryNodeMap.containsKey(queryGraphObjectLabel)) {
                result.addNodeBinding(queryGraphObjectLabel, objectNodeBinding);
            }
            results.add(result);
        }
        return results;
    }

/*
    private List<Result> completeResults(List<Result> inputList) {
        List<Result> outputList = new ArrayList<>();
        for (int i = 0; i < inputList.size(); i++) {
            Result first = inputList.get(i);
            Map<String, List<EdgeBinding>> firstEdges = first.getEdgeBindings();
            Map<String, List<NodeBinding>> firstNodes = first.getNodeBindings();
            List<Integer> mergeIndices = new ArrayList<>();
            for (int j = i + 1; j < inputList.size(); j++) {
                Result second = inputList.get(j);
                Map<String, List<EdgeBinding>> secondEdges = second.getEdgeBindings();
                Map<String, List<NodeBinding>> secondNodes = second.getNodeBindings();
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
                Map<String, List<NodeBinding>> nodeMap = new HashMap<>(firstNodes);
                Map<String, List<EdgeBinding>> edgeMap = new HashMap<>(firstEdges);
                for (Integer index : mergeIndices) {
                    Map<String, List<EdgeBinding>> secondEdges = inputList.get(index).getEdgeBindings();
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
*/
    // endregion

    //region Just Overlay Stuff

    private Metrics getMetrics(String concept1, String concept2, String part) {
        int singleCount1, singleCount2, pairCount, totalConceptCount, totalDocumentCount;
        totalConceptCount = nodeRepo.getTotalConceptCount();
        totalDocumentCount = nodeRepo.getDocumentCount(part);
        Map<String, Map<String, Integer>> singleCountsMap = lookupQueries.getSingleCounts(List.of(concept1, concept2));
        singleCount1 = singleCountsMap.getOrDefault(concept1, Collections.emptyMap()).getOrDefault(part, 0);
        singleCount2 = singleCountsMap.getOrDefault(concept2, Collections.emptyMap()).getOrDefault(part, 0);
        // TODO: Make a single concept pair version of getCooccurrences
        Map<String, List<String>> cooccurrences = lookupQueries.getPairCounts(Collections.singletonList(concept1), Collections.singletonList(concept2));
        List<String> documents = cooccurrences.getOrDefault(concept1 + concept2 + part, Collections.emptyList());
        pairCount = documents.size();
        Metrics metrics = new Metrics(singleCount1, singleCount2, pairCount, totalConceptCount, totalDocumentCount, part);
        if (pairCount > 0) {
            metrics.setDocumentIdList(documents.stream().map(hash -> hash.split("_")[0]).collect(Collectors.toList()));
        }
        return metrics;
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
    private KnowledgeGraph buildKnowledgeGraph(List<ConceptPair> conceptPairs, Map<String, String> labelMap, Map<String, List<String>> categoryMap, JsonNode normalizedNodes) {
        KnowledgeGraph kg = new KnowledgeGraph();
        for (ConceptPair pair : conceptPairs) {
            List<Attribute> attributeList = pair.toAttributeList();
            if (attributeList.size() < 2) {
                continue;
            }
            String subjectLabel = labelMap.getOrDefault(pair.getSubject(), "");
            String objectLabel = labelMap.getOrDefault(pair.getObject(), "");
            String cat;
            List<String> subjectCategoryList = categoryMap.getOrDefault(pair.getSubject(), new ArrayList<>());
            List<String> objectCategoryList = categoryMap.getOrDefault(pair.getObject(), new ArrayList<>());
            if (subjectLabel.isEmpty()) {
                if (normalizedNodes.hasNonNull(pair.getSubject()) && normalizedNodes.get(pair.getSubject()).hasNonNull("id") &&
                        normalizedNodes.get(pair.getSubject()).get("id").hasNonNull("label")) {
                    subjectLabel = normalizedNodes.get(pair.getSubject()).get("id").get("label").asText();
                }
            }
            if (objectLabel.isEmpty()) {
                if (normalizedNodes.hasNonNull(pair.getObject()) && normalizedNodes.get(pair.getObject()).hasNonNull("id") &&
                        normalizedNodes.get(pair.getObject()).get("id").hasNonNull("label")) {
                    objectLabel = normalizedNodes.get(pair.getObject()).get("id").get("label").asText();
                }
            }
            if (subjectCategoryList.size() == 0) {
                if (normalizedNodes.hasNonNull(pair.getSubject()) && normalizedNodes.get(pair.getSubject()).hasNonNull("type") &&
                        normalizedNodes.get(pair.getSubject()).get("type").isArray()) {
                    Iterator<JsonNode> cats = normalizedNodes.get(pair.getSubject()).get("type").elements();
                    while (cats.hasNext()) {
                        JsonNode categoryNode = cats.next();
                        cat = categoryNode.asText();
                        if (!invalidClasses.contains(cat.toLowerCase())) {
                            subjectCategoryList.add(cat);
                        }
                    }
                }
            } else {
                List<String> removeList = new ArrayList<>();
                for (String category : subjectCategoryList) {
                    if (invalidClasses.contains(category.toLowerCase())) {
                        removeList.add(category);
                    }
                }
                subjectCategoryList.removeAll(removeList);
            }
            if (objectCategoryList.size() == 0) {
                if (normalizedNodes.hasNonNull(pair.getObject()) && normalizedNodes.get(pair.getObject()).hasNonNull("type") &&
                        normalizedNodes.get(pair.getObject()).get("type").isArray()) {
                    Iterator<JsonNode> cats = normalizedNodes.get(pair.getObject()).get("type").elements();
                    while (cats.hasNext()) {
                        JsonNode categoryNode = cats.next();
                        cat = categoryNode.asText();
                        if (!invalidClasses.contains(cat.toLowerCase())) {
                            objectCategoryList.add(cat);
                        }
                    }
                }
            } else {
                List<String> removeList = new ArrayList<>();
                for (String category : objectCategoryList) {
                    if (invalidClasses.contains(category.toLowerCase())) {
                        removeList.add(category);
                    }
                }
                objectCategoryList.removeAll(removeList);
            }
            KnowledgeEdge edge = new KnowledgeEdge(pair.getSubject(), pair.getObject(), "biolink:occurs_together_in_literature_with", attributeList);
            KnowledgeNode subjectNode = new KnowledgeNode(subjectLabel, subjectCategoryList);
            KnowledgeNode objectNode = new KnowledgeNode(objectLabel, objectCategoryList);

            RetrievalSource source = new RetrievalSource();
            source.setResource("infores:text-mining-provider-cooccurrence");
            source.setResourceRole("primary_knowledge_source");
            edge.addSource(source);

            // These are the only way I could think of to know which kedge and knode is connected to which qedge and qnode
            edge.setQueryKey(pair.getEdgeKey());
            subjectNode.setQueryKey(pair.getSubjectKey());
            objectNode.setQueryKey(pair.getObjectKey());

            kg.addEdge(pair.getSubject() + "_" + pair.getObject(), edge);
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
        List<String> subjectCuries;
        List<String> objectCuries;
        if (subjectCategoryQuery) {
            subjectCuries = lookupQueries.getCuriesForCategory(subjectCategory);
        } else {
            subjectCuries = getTextMinedCuries(subjectCurieList);
        }
        if (objectCategoryQuery) {
            objectCuries = lookupQueries.getCuriesForCategory(objectCategory);
        } else {
            objectCuries = getTextMinedCuries(objectCurieList);
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
                ConceptPair pair = new ConceptPair(sub, obj);
                for (String part : documentParts) {
                    if (pairCounts.containsKey(sub + obj + part)) {
                        int pairCount = pairCounts.get(sub + obj + part);
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
                        pair.setPairMetrics(part, metrics);
                    }
                    conceptPairs.add(pair);
                }
            }
        }
        return conceptPairs;
    }

    // The goal here is to translate as many incoming curies as possible to the curies used in the text mined database.
    private List<String> getTextMinedCuries(List<String> queryCuriesList) {
        logger.debug("Starting curie(s)");
        logger.debug(String.join(",", queryCuriesList));
        // The first step is to get those curies that are already known synonyms of TM curies
        Map<String, List<String>> textMinedCuriesMap = lookupQueries.getTextMinedCuriesMap(queryCuriesList);
        List<String> textMinedCuriesList = new ArrayList<>(queryCuriesList.size());
        List<String> unmatchedQueryCuries = new ArrayList<>();
        for (String queryCurie : queryCuriesList) {
            if (textMinedCuriesMap.containsKey(queryCurie)) {
                textMinedCuriesList.addAll(textMinedCuriesMap.get(queryCurie));
            } else {
                unmatchedQueryCuries.add(queryCurie);
            }
        }
        // If anything doesn't match in the synonym table we check Node Normalizer (because NN is apparently not symmetrical).
        if (unmatchedQueryCuries.size() > 0) {
            logger.debug("Trying SRI NN");
            List<List<String>> newSynonymsList = new ArrayList<>();
            List<String> allTextMinedCuries = lookupQueries.getTextMinedCuries();
            JsonNode nnJSON = sri.getNormalizedNodesInBatches(unmatchedQueryCuries, NN_BATCH_SIZE);
            logger.debug(nnJSON.toPrettyString());
            for (String curie : unmatchedQueryCuries) {
                logger.debug("Trying for " + curie);
                boolean foundInNN = false;
                List<String> synonyms = sri.getNodeSynonyms(curie, nnJSON);
                if (synonyms.size() > 0) {
                    logger.debug("NN curie(s)");
                    logger.debug(String.join(",", synonyms));
                    for (String synonym : synonyms) {
                        if (allTextMinedCuries.contains(synonym)) {
                            logger.debug(synonym + " is in TM");
                            foundInNN = true;
                            textMinedCuriesList.add(synonym);
                            newSynonymsList.add(List.of(synonym, curie));
                        }
                    }
                } else {
                    logger.debug("No synonyms found");
                }
                if (foundInNN) { // If the NN lookup found matching curies we add those synonyms into the table for next time.
                    lookupQueries.addSynonyms(newSynonymsList);
                } else { // Otherwise just add the original curie back into the list to be queried.
                    textMinedCuriesList.add(curie); // TODO: decide if it would be better just to skip curies that don't exist in the TM nodes
                }
            }
        }
        logger.debug("Resulting curie(s)");
        logger.debug(String.join(",", textMinedCuriesList));
        return textMinedCuriesList;
    }

    private Map<String, List<String>> getPairCounts(Map<String, List<String>> subjectHierarchy, Map<String, List<String>> objectHierarchy) {
        Map<String, List<String>> cooccurrences = lookupQueries.getCooccurrencesByParts(new ArrayList<>(subjectHierarchy.keySet()), new ArrayList<>(objectHierarchy.keySet()));
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
        return cooccurrences;
    }

    // endregion
}
