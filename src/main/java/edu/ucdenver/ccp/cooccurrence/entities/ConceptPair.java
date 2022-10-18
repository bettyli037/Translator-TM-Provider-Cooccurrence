package edu.ucdenver.ccp.cooccurrence.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.ucdenver.ccp.cooccurrence.TRAPI.AttributeConstraint;

import java.math.BigInteger;
import java.util.List;
import java.util.regex.Pattern;

public class ConceptPair {

    private String subject;
    private String object;
    private int pairCount;
    private String documentHash;
    private String part;
    private String category;
    private String subjectKey;
    private String objectKey;
    private String edgeKey;

    private Metrics pairMetrics;

    public ConceptPair(String subject, String object, String part, BigInteger pairCount) {
        this.subject = subject;
        this.object = object;
        this.part = part;
        this.pairCount = pairCount.intValue();
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

    public int getPairCount() {
        return pairCount;
    }

    public void setPairCount(int pairCount) {
        this.pairCount = pairCount;
    }

    public String getDocumentHash() {
        return documentHash;
    }

    public void setDocumentHash(String documentHash) {
        this.documentHash = documentHash;
    }

    public String getPart() {
        return part;
    }

    public void setPart(String part) {
        this.part = part;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
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

    public Metrics getPairMetrics() {
        return pairMetrics;
    }

    public void setPairMetrics(Metrics pairMetrics) {
        this.pairMetrics = pairMetrics;
    }

    public boolean meetsConstraints(List<AttributeConstraint> constraintList) {
        boolean overallPass = false;
        for (AttributeConstraint constraint : constraintList) {
            overallPass = overallPass || meetsConstraint(constraint);
        }
        return overallPass;
    }

    private boolean meetsConstraint(AttributeConstraint constraint) {
        if (!AttributeConstraint.supportedAttributes.contains(constraint.getId())) {
            return false;
        }
        boolean isList = constraint.getValue().isArray(), isNot = constraint.isNot();
        String attributeString = null, constraintString;
        double attributeValue, constraintValue;
        switch (constraint.getId()) {
            case "biolink:concept_count_subject":
                attributeValue = this.pairMetrics.getSingleCount1();
                break;
            case "biolink:concept_count_object":
                attributeValue = this.pairMetrics.getSingleCount2();
                break;
            case "biolink:tmkp_concept_pair_count":
                attributeValue = this.pairMetrics.getPairCount();
                break;
            case "biolink:tmkp_normalized_google_distance":
                attributeValue = this.pairMetrics.getNormalizedGoogleDistance();
                break;
            case "biolink:tmkp_pointwise_mutual_information":
                attributeValue = this.pairMetrics.getPointwiseMutualInformation();
                break;
            case "biolink:tmkp_normalized_pointwise_mutual_information":
                attributeValue = this.pairMetrics.getNormalizedPointwiseMutualInformation();
                break;
            case "biolink:tmkp_mutual_dependence":
                attributeValue = this.pairMetrics.getMutualDependence();
                break;
            case "biolink:tmkp_normalized_pointwise_mutual_information_max_denominator":
                attributeValue = this.pairMetrics.getNormalizedPointwiseMutualInformationMaxDenom();
                break;
            case "biolink:tmkp_log_frequency_biased_mutual_dependence":
                attributeValue = this.pairMetrics.getLogFrequencyBiasedMutualDependence();
                break;
            case "biolink:tmkp_document_part":
                attributeValue = Double.NEGATIVE_INFINITY;
                attributeString = this.part;
                break;
            default:
                attributeValue = Double.NEGATIVE_INFINITY;
        }
        // None of the attributes are list-valued, so if the constraint is a list and the operator is "===" the comparison is necessarily false
        if (isList) {
            for (JsonNode item : constraint.getValue()) {
                if (constraint.getId().equals("biolink:tmkp_document_part")) {
                    // biolink:tmkp_document_part is currently the only string attribute, so we handle it specially
                    constraintString = item.asText();
                    boolean comparisonResult = false;
                    if (attributeString != null) {
                        if (constraint.getOperator().equals("==")) {
                            comparisonResult = attributeString.equals(constraintString);
                        } else if (constraint.getOperator().equals("matches")) {
                            comparisonResult = Pattern.matches(constraintString, attributeString);
                        }
                        if ((!isNot && comparisonResult) || (isNot && !comparisonResult)) {
                            return true;
                        }
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
            if (constraint.getId().equals("biolink:tmkp_document_part")) {
                // biolink:tmkp_document_part is currently the only string attribute, so we handle it specially
                constraintString = constraint.getValue().asText();
                if (attributeString == null) {
                    return false;
                }
                switch (constraint.getOperator()) {
                    case "==":
                        comparisonResult = attributeString.equals(constraintString);
                        break;
                    case "===":
                        comparisonResult = attributeString.equals(constraintString) && constraint.getValue().isTextual();
                        break;
                    case "matches":
                        comparisonResult = Pattern.matches(constraintString, attributeString);
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


    public JsonNode toJson() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = mapper.createObjectNode();
        node.put("subject", this.subject);
        node.put("q_subject", this.subjectKey);
        node.put("object", this.object);
        node.put("q_object", this.objectKey);
        node.put("q_edge", this.edgeKey);
        node.put("document_part", this.part);
        if (pairMetrics != null) {
            node.set("metrics", pairMetrics.toJSON());
        }
        return node;
    }

    public String toString() {
        return this.toJson().toPrettyString();
    }
}