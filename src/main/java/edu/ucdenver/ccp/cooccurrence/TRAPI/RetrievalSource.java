package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class RetrievalSource {
    private String resource;
    private String resourceRole;
    private final List<String> upstreamResources;
    private final List<String> sourceRecordUrls;
    private final Map<String, JsonNode> additionalProperties;

    // region Boilerplate
    public RetrievalSource() {
        resource = "";
        resourceRole = "";
        upstreamResources = new ArrayList<>();
        sourceRecordUrls = new ArrayList<>();
        additionalProperties = new HashMap<>();
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getResourceRole() {
        return resourceRole;
    }

    public void setResourceRole(String resourceRole) {
        this.resourceRole = resourceRole;
    }

    public List<String> getUpstreamResources() {
        return upstreamResources;
    }

    public List<String> getSourceRecordUrls() {
        return sourceRecordUrls;
    }

    public Map<String, JsonNode> getAdditionalProperties() {
        return additionalProperties;
    }

    public void addUpstreamResource(String upstreamResource) {
        upstreamResources.add(upstreamResource);
    }

    public void addSourceRecordUrl(String sourceRecordUrl) {
        sourceRecordUrls.add(sourceRecordUrl);
    }

    public void addAdditionalProperty(String key, JsonNode value) {
        additionalProperties.put(key, value);
    }

    // endregion

    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode edgeNode = om.createObjectNode();
        edgeNode.put("resource", this.resource);
        edgeNode.put("resource_role", this.resourceRole);
        if (this.upstreamResources.size() > 0) {
            edgeNode.set("upstream_resources", om.valueToTree(this.upstreamResources));
        }
        if (this.sourceRecordUrls.size() > 0) {
            edgeNode.set("source_record_urls", om.valueToTree(this.sourceRecordUrls));
        }
        for (Map.Entry<String, JsonNode> kv : this.additionalProperties.entrySet()) {
            edgeNode.set(kv.getKey(), kv.getValue());
        }
        return edgeNode;
    }
    public static RetrievalSource parseJSON(JsonNode json) {
        if (!json.hasNonNull("resource") || !json.hasNonNull("resource_role")) {
            return null;
        }
        RetrievalSource retrievalSource = new RetrievalSource();
        retrievalSource.setResource(json.get("resource").asText());
        retrievalSource.setResourceRole(json.get("resource_role").asText());

        Iterator<String> keyIterator = json.fieldNames();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (key.equals("nodes") || key.equals("edges")) {
                continue;
            }
            retrievalSource.addAdditionalProperty(key, json.get(key));
        }

        return retrievalSource;
    }
}

