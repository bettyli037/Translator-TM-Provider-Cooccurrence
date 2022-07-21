package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QueryEdge {
    private List<String> predicates;
    private String subject;
    private String object;
    private List<AttributeConstraint> constraints;

    public QueryEdge() {
        predicates = new ArrayList<>();
        constraints = new ArrayList<>();
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

    public List<AttributeConstraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<AttributeConstraint> constraints) {
        this.constraints = constraints;
    }

    public void addConstraint(AttributeConstraint constraint) {
        this.constraints.add(constraint);
    }

    public JsonNode toJSON() {
        return (new ObjectMapper()).convertValue(this, ObjectNode.class);
    }

    public static QueryEdge parseJSON(JsonNode jsonQEdge) {
        if (!jsonQEdge.hasNonNull("subject") || !jsonQEdge.hasNonNull("object")) {
            return null;
        }
        QueryEdge edge = new QueryEdge();
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

        if (jsonQEdge.hasNonNull("constraints") && jsonQEdge.get("constraints").isArray()) {
            JsonNode constraintsNode = jsonQEdge.get("constraints");
            Iterator<JsonNode> constraintIterator = constraintsNode.elements();
            while (constraintIterator.hasNext()) {
                JsonNode constraintNode = constraintIterator.next();
                edge.addConstraint(AttributeConstraint.parseJSON(constraintNode));
            }
        }

        edge.setSubject(jsonQEdge.get("subject").asText());
        edge.setObject(jsonQEdge.get("object").asText());
        return edge;
    }
}
