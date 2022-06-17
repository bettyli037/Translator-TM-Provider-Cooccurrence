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
    private List<Object> constraints;

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

    public List<Object> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<Object> constraints) {
        this.constraints = constraints;
    }

    public JsonNode toJSON() {
        return (new ObjectMapper()).convertValue(this, ObjectNode.class);
    }

    public static QueryEdge parseJSON(JsonNode jsonQEdge) {
        if (!jsonQEdge.hasNonNull("subject") || !jsonQEdge.hasNonNull("object")) {
            return null;
        }
        QueryEdge edge = new QueryEdge();
        if (jsonQEdge.hasNonNull("predicate")) {
            JsonNode predicatesNode = jsonQEdge.get("predicate");
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
        return edge;
    }
}
