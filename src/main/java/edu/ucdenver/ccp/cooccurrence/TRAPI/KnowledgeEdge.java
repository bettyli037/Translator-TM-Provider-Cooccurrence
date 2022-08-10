package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

public class KnowledgeEdge {
    private String subject;
    private String object;
    private String predicate;
    private List<Attribute> attributes;
    private List<Qualifier> qualifiers;

    private String queryKey;

    public KnowledgeEdge(String subject, String object, String predicate, List<Attribute> attributes) {
        this.subject = subject;
        this.object = object;
        this.predicate = predicate;
        this.attributes = attributes;
        this.qualifiers = new ArrayList<>();
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

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<Attribute> attributes) {
        this.attributes = attributes;
    }

    public List<Qualifier> getQualifiers() {
        return qualifiers;
    }

    public void setQualifiers(List<Qualifier> qualifiers) {
        this.qualifiers = qualifiers;
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
        if (this.attributes.size() > 0) {
            ArrayNode attributesNode = om.createArrayNode();
            for (Attribute attribute : this.attributes) {
                attributesNode.add(attribute.toJSON());
            }
            edgeNode.set("attributes", attributesNode);
        }
        if (this.qualifiers.size() > 0) {
            ArrayNode qualifiersNode = om.createArrayNode();
            for (Qualifier qualifier : this.qualifiers) {
                qualifiersNode.add(qualifier.toJSON());
            }
            edgeNode.set("qualifiers", qualifiersNode);
        }
        return edgeNode;
    }
}
