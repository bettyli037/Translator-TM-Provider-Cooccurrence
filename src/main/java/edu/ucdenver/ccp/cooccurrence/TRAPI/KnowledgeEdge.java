package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class KnowledgeEdge {
    private String subject;
    private String object;
    private String predicate;
    private ArrayNode attributes;

    private String queryKey;

    public KnowledgeEdge(String subject, String object, String predicate, ArrayNode attributes) {
        this.subject = subject;
        this.object = object;
        this.predicate = predicate;
        this.attributes = attributes;
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

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public ArrayNode getAttributes() {
        return attributes;
    }

    public void setAttributes(ArrayNode attributes) {
        this.attributes = attributes;
    }

    public String getQueryKey() {
        return queryKey;
    }

    public void setQueryKey(String key) {
        this.queryKey = key;
    }

    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode edgeNode = om.createObjectNode();
        edgeNode.put("subject", this.subject);
        edgeNode.put("object", this.object);
        edgeNode.put("predicate", this.predicate);
        edgeNode.set("attributes", this.attributes);
        return edgeNode;
    }
}
