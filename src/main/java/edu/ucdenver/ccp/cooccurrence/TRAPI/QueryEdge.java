package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class QueryEdge {
    private String knowledgeType;
    private List<String> predicates;
    private String subject;
    private String object;
    private List<AttributeConstraint> attributeConstraints;
    private List<QualifierConstraint> qualifierConstraints;
    private Map<String, JsonNode> additionalProperties;

    public QueryEdge() {
        knowledgeType = null;
        predicates = new ArrayList<>();
        attributeConstraints = new ArrayList<>();
        qualifierConstraints = new ArrayList<>();
        additionalProperties = new HashMap<>();
    }

    public List<String> getPredicates() {
        return predicates;
    }

    public void setPredicates(List<String> predicates) {
        this.predicates = predicates;
    }

    public void addPredicate(String predicate) {
        predicates.add(predicate);
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

    public void setObject(String object) {
        this.object = object;
    }

    public List<AttributeConstraint> getAttributeConstraints() {
        return attributeConstraints;
    }

    public void setAttributeConstraints(List<AttributeConstraint> constraints) {
        this.attributeConstraints = constraints;
    }

    public void addAttributeConstraint(AttributeConstraint constraint) {
        this.attributeConstraints.add(constraint);
    }

    public String getKnowledgeType() {
        return knowledgeType;
    }

    public void setKnowledgeType(String knowledgeType) {
        this.knowledgeType = knowledgeType;
    }

    public List<QualifierConstraint> getQualifierConstraints() {
        return qualifierConstraints;
    }

    public void setQualifierConstraints(List<QualifierConstraint> qualifierConstraints) {
        this.qualifierConstraints = qualifierConstraints;
    }

    public void addQualifierConstraint(QualifierConstraint constraint) {
        this.qualifierConstraints.add(constraint);
    }

    public Map<String, JsonNode> getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Map<String, JsonNode> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public void addAdditionalProperty(String key, JsonNode value) {
        this.additionalProperties.put(key, value);
    }

    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode edgeNode = om.createObjectNode();
        if (this.knowledgeType != null) {
            edgeNode.put("knowledge_type", this.knowledgeType);
        }
        if (this.predicates.size() > 0) {
            edgeNode.set("predicates", om.convertValue(this.predicates, ArrayNode.class));
        }
        edgeNode.put("subject", this.subject);
        edgeNode.put("object", this.object);
        if (this.attributeConstraints.size() > 0) {
            ArrayNode constraintArrayNode = om.createArrayNode();
            for (AttributeConstraint constraint : this.attributeConstraints) {
                constraintArrayNode.add(constraint.toJSON());
            }
            edgeNode.set("attribute_constraints", constraintArrayNode);
        }
        if (this.qualifierConstraints.size() > 0) {
            ArrayNode constraintArrayNode = om.createArrayNode();
            for (QualifierConstraint constraint : this.qualifierConstraints) {
                constraintArrayNode.add(constraint.toJSON());
            }
            edgeNode.set("qualifier_constraints", constraintArrayNode);
        }
        for (Map.Entry<String, JsonNode> kv : this.additionalProperties.entrySet()) {
            edgeNode.set(kv.getKey(), kv.getValue());
        }
        return edgeNode;
    }

    public static QueryEdge parseJSON(JsonNode jsonQEdge) {
        if (!jsonQEdge.hasNonNull("subject") || !jsonQEdge.hasNonNull("object")) {
            return null;
        }
        QueryEdge edge = new QueryEdge();

        if (jsonQEdge.hasNonNull("knowledge_type")) {
            edge.setKnowledgeType(jsonQEdge.get("knowledge_type").asText());
        }

        if (jsonQEdge.hasNonNull("predicate") || jsonQEdge.hasNonNull("predicates")) {
            JsonNode predicatesNode = jsonQEdge.hasNonNull("predicate") ? jsonQEdge.get("predicate") : jsonQEdge.get("predicates");
            if (predicatesNode.isArray()) {
                Iterator<JsonNode> predicates = predicatesNode.elements();
                while (predicates.hasNext()) {
                    JsonNode predicate = predicates.next();
                    edge.addPredicate(predicate.asText());
                }
            } else if (predicatesNode.isValueNode()) {
                edge.addPredicate(predicatesNode.asText());
            } else {
                return null;
            }
        }

        edge.setSubject(jsonQEdge.get("subject").asText());
        edge.setObject(jsonQEdge.get("object").asText());

        if (jsonQEdge.hasNonNull("attribute_constraints") && jsonQEdge.get("attribute_constraints").isArray()) {
            JsonNode constraintsNode = jsonQEdge.get("attribute_constraints");
            Iterator<JsonNode> constraintIterator = constraintsNode.elements();
            while (constraintIterator.hasNext()) {
                JsonNode constraintNode = constraintIterator.next();
                edge.addAttributeConstraint(AttributeConstraint.parseJSON(constraintNode));
            }
        }

        if (jsonQEdge.hasNonNull("qualifier_constraints") && jsonQEdge.get("qualifier_constraints").isArray()) {
            JsonNode constraintsNode = jsonQEdge.get("qualifier_constraints");
            Iterator<JsonNode> constraintIterator = constraintsNode.elements();
            while (constraintIterator.hasNext()) {
                JsonNode constraintNode = constraintIterator.next();
                edge.addQualifierConstraint(QualifierConstraint.parseJSON(constraintNode));
            }
        }

        List<String> excludedKeys = List.of("knowledge_type", "predicates", "predicate", "subject", "object", "attribute_constraints", "qualifier_constraints");
        Iterator<String> keyIterator = jsonQEdge.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (excludedKeys.contains(key)) {
                continue;
            }
            edge.addAdditionalProperty(key, jsonQEdge.get(key));
        }

        return edge;
    }
}
