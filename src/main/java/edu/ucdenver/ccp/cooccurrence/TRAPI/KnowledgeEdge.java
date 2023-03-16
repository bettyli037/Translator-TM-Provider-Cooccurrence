package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KnowledgeEdge {
    private String subject;
    private String object;
    private String predicate;
    private List<Attribute> attributes;
    private List<Qualifier> qualifiers;
    private String queryKey;

    public KnowledgeEdge() {
        this.subject = "";
        this.object = "";
        this.predicate = "";
        this.attributes = new ArrayList<>();
        this.qualifiers = new ArrayList<>();
        this.queryKey = "";
    }

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

    public void addAttribute(Attribute attribute) {
        this.attributes.add(attribute);
    }

    public List<Qualifier> getQualifiers() {
        return qualifiers;
    }

    public void setQualifiers(List<Qualifier> qualifiers) {
        this.qualifiers = qualifiers;
    }

    public void addQualifier(Qualifier qualifier) {
        this.qualifiers.add(qualifier);
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
    public static KnowledgeEdge parseJSON(JsonNode jsonKEdge) {
        if (!jsonKEdge.hasNonNull("subject") || !jsonKEdge.hasNonNull("object")) {
            return null;
        }
        KnowledgeEdge edge = new KnowledgeEdge();
        edge.setSubject(jsonKEdge.get("subject").asText());
        edge.setObject(jsonKEdge.get("object").asText());
        if (jsonKEdge.hasNonNull("predicate")) {
            edge.setPredicate(jsonKEdge.get("predicate").asText());
        }

        if (jsonKEdge.hasNonNull("attributes") && jsonKEdge.get("attributes").isArray()) {
            JsonNode attributesNode = jsonKEdge.get("attributes");
            Iterator<JsonNode> attributesIterator = attributesNode.elements();
            while (attributesIterator.hasNext()) {
                edge.addAttribute(Attribute.parseJSON(attributesIterator.next()));
            }
        }

        if (jsonKEdge.hasNonNull("qualifiers") && jsonKEdge.get("qualifiers").isArray()) {
            JsonNode qualifiersNode = jsonKEdge.get("qualifiers");
            Iterator<JsonNode> qualifiersIterator = qualifiersNode.elements();
            while (qualifiersIterator.hasNext()) {
                edge.addQualifier(Qualifier.parseJSON(qualifiersIterator.next()));
            }
        }

        return edge;
    }
}