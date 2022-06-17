package edu.ucdenver.ccp.cooccurrence.entities;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigInteger;

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