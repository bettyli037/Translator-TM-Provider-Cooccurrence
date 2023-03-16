package edu.ucdenver.ccp.cooccurrence.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.ucdenver.ccp.cooccurrence.TRAPI.Attribute;
import edu.ucdenver.ccp.cooccurrence.TRAPI.AttributeConstraint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ConceptPair {

    private String subject;
    private String object;
    private String subjectKey;
    private String objectKey;
    private String edgeKey;
    private final Map<String, Metrics> metricsMap;

    public ConceptPair(String subject, String object) {
        this.subject = subject;
        this.object = object;
        this.metricsMap = new HashMap<>(4);
    }

    public ConceptPair(String subject, String object, String subjectKey, String objectKey, String edgeKey) {
        this.subject = subject;
        this.object = object;
        this.subjectKey = subjectKey;
        this.objectKey = objectKey;
        this.edgeKey = edgeKey;
        this.metricsMap = new HashMap<>(4);
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String concept) {
        this.object = concept;
    }

    public void setKeys(String subjectKey, String objectKey, String edgeKey) {
        this.subjectKey = subjectKey;
        this.objectKey = objectKey;
        this.edgeKey = edgeKey;
    }

    public String getSubjectKey() {
        return subjectKey;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public String getEdgeKey() {
        return edgeKey;
    }

    public Metrics getPairMetrics(String documentPart) {
        return this.metricsMap.getOrDefault(documentPart, null);
    }

    public void setPairMetrics(String documentPart, Metrics pairMetrics) {
        this.metricsMap.put(documentPart, pairMetrics);
    }

    // This method removes parts from the metricsMap that do not satisfy the given constraint.
    // If none of the metrics meet the constraints, this returns null
    public ConceptPair satisfyConstraint(AttributeConstraint constraint) {
        ConceptPair constrainedConceptPair = new ConceptPair(this.subject, this.object, this.subjectKey, this.objectKey, this.edgeKey);
        int partsCount = 0;
        for (String part : metricsMap.keySet()) {
            if (meetsConstraint(constraint, part)) {
                partsCount++;
                constrainedConceptPair.setPairMetrics(part, metricsMap.get(part));
            }
        }
        if (partsCount == 0) {
            return null;
        }
        return constrainedConceptPair;
    }

    private boolean meetsConstraint(AttributeConstraint constraint, String part) {
        if (!AttributeConstraint.supportedAttributes.contains(constraint.getId())) {
            return false;
        }
        if (!this.metricsMap.containsKey(part)) {
            return false;
        }
        boolean isList = constraint.getValue().isArray(), isNot = constraint.isNot();
        String attributeString = null, constraintString;
        double attributeValue, constraintValue;
        Metrics metrics = this.metricsMap.get(part);
        switch (constraint.getId()) {
            case "biolink:concept_count_subject":
                attributeValue = metrics.getSingleCount1();
                break;
            case "biolink:concept_count_object":
                attributeValue = metrics.getSingleCount2();
                break;
            case "biolink:tmkp_concept_pair_count":
                attributeValue = metrics.getPairCount();
                break;
            case "biolink:tmkp_normalized_google_distance":
                attributeValue = metrics.getNormalizedGoogleDistance();
                break;
            case "biolink:tmkp_pointwise_mutual_information":
                attributeValue = metrics.getPointwiseMutualInformation();
                break;
            case "biolink:tmkp_normalized_pointwise_mutual_information":
                attributeValue = metrics.getNormalizedPointwiseMutualInformation();
                break;
            case "biolink:tmkp_mutual_dependence":
                attributeValue = metrics.getMutualDependence();
                break;
            case "biolink:tmkp_normalized_pointwise_mutual_information_max_denominator":
                attributeValue = metrics.getNormalizedPointwiseMutualInformationMaxDenom();
                break;
            case "biolink:tmkp_log_frequency_biased_mutual_dependence":
                attributeValue = metrics.getLogFrequencyBiasedMutualDependence();
                break;
            default:
                attributeValue = Double.NEGATIVE_INFINITY;
        }
        // None of the attributes are list-valued, so if the constraint is a list and the operator is "===" the comparison is necessarily false
        if (isList) {
            for (JsonNode item : constraint.getValue()) {
                if (constraint.getId().equals("biolink:supporting_text_located_in")) {
                    // biolink:supporting_text_located_in is currently the only string attribute, so we handle it specially
                    constraintString = item.asText();
                    boolean comparisonResult = false;
                    if (constraint.getOperator().equals("==")) {
                        comparisonResult = part.equals(constraintString);
                    } else if (constraint.getOperator().equals("matches")) {
                        comparisonResult = Pattern.matches(constraintString, part);
                    }
                    if ((!isNot && comparisonResult) || (isNot && !comparisonResult)) {
                        return true;
                    }
                } else {
                    constraintValue = item.asDouble();
                    boolean comparisonResult = false;
                    switch (constraint.getOperator()) {
                        case "==":
                            comparisonResult = attributeValue == constraintValue;
                            break;
                        case ">":
                            comparisonResult = attributeValue > constraintValue;
                            break;
                        case "<":
                            comparisonResult = attributeValue < constraintValue;
                            break;
                    }
                    if ((!isNot && comparisonResult) || (isNot && !comparisonResult)) {
                        return true;
                    }
                }
            }
            return false;
        } else {
            boolean comparisonResult = false;
            if (constraint.getId().equals("biolink:supporting_text_located_in")) {
                // biolink:supporting_text_located_in is currently the only string attribute, so we handle it specially
                constraintString = constraint.getValue().asText();
                switch (constraint.getOperator()) {
                    case "==":
                        comparisonResult = part.equals(constraintString);
                        break;
                    case "===":
                        comparisonResult = part.equals(constraintString) && constraint.getValue().isTextual();
                        break;
                    case "matches":
                        comparisonResult = Pattern.matches(constraintString, part);
                        break;
                }
                return (!isNot && comparisonResult) || (isNot && !comparisonResult);
            }
            constraintValue = constraint.getValue().asDouble();
            switch (constraint.getOperator()) {
                case "==":
                    comparisonResult = attributeValue == constraintValue;
                    break;
                case ">":
                    comparisonResult = attributeValue > constraintValue;
                    break;
                case "<":
                    comparisonResult = attributeValue < constraintValue;
                    break;
                case "===":
                    // Using isIntegralNumber and isFloatingPointNumber to avoid differentiating between int/short/long or float/double when comparing types
                    boolean typeComparison;
                    if (List.of("biolink:concept_count_subject", "biolink:concept_count_object", "biolink:tmkp_concept_pair_count").contains(constraint.getId())) {
                        typeComparison = constraint.getValue().isIntegralNumber();
                    } else {
                        typeComparison = constraint.getValue().isFloatingPointNumber();
                    }
                    comparisonResult = typeComparison && (attributeValue == constraintValue);
                    break;
            }
            return (!isNot && comparisonResult) || (isNot && !comparisonResult);
        }
    }

    public List<Attribute> toAttributeList() {
        List<Attribute> attributeList = new ArrayList<>(metricsMap.size());
        for (Map.Entry<String, Metrics> metricsEntry : metricsMap.entrySet()) {
            Attribute metricsAttribute = new Attribute();
            metricsAttribute.setAttributeTypeId("biolink:has_supporting_study_result");
            metricsAttribute.setAttributeSource("infores:text-mining-provider-cooccurrence");
            List<Attribute> subAttributesList = metricsEntry.getValue().toAttributeList();
            if (subAttributesList.size() == 0) {
                continue;
            }
            subAttributesList.forEach(metricsAttribute::addAttribute);
            attributeList.add(metricsAttribute);
        }
        return attributeList;
    }

    public JsonNode toJson() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("subject", this.subject);
        node.put("q_subject", this.subjectKey);
        node.put("object", this.object);
        node.put("q_object", this.objectKey);
        node.put("q_edge", this.edgeKey);
        ObjectNode metricsNode = mapper.createObjectNode();
        for (Map.Entry<String, Metrics> entry : this.metricsMap.entrySet()) {
            metricsNode.set(entry.getKey(), entry.getValue().toJSON());
        }
        node.set("metrics", metricsNode);
        return node;
    }

    public String toString() {
        return this.toJson().toPrettyString();
    }
}