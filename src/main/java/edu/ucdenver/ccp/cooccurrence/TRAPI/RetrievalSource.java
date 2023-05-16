package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class RetrievalSource {
    private String resource;
    private String resourceRole;
    private final List<String> upstreamResourceIds;
    private final List<String> sourceRecordUrls;
    private final Map<String, JsonNode> additionalProperties;

    // region Boilerplate
    public RetrievalSource() {
        resource = "";
        resourceRole = "";
        upstreamResourceIds = new ArrayList<>();
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

    public List<String> getUpstreamResourceIds() {
        return upstreamResourceIds;
    }

    public List<String> getSourceRecordUrls() {
        return sourceRecordUrls;
    }

    public Map<String, JsonNode> getAdditionalProperties() {
        return additionalProperties;
    }

    public void addUpstreamResourceId(String upstreamResourceId) {
        upstreamResourceIds.add(upstreamResourceId);
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
        edgeNode.put("resource_id", this.resource);
        edgeNode.put("resource_role", this.resourceRole);
        if (this.upstreamResourceIds.size() > 0) {
            edgeNode.set("upstream_resource_ids", om.valueToTree(this.upstreamResourceIds));
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
        if (!json.hasNonNull("resource_id") || !json.hasNonNull("resource_role")) {
            return null;
        }
        RetrievalSource retrievalSource = new RetrievalSource();
        retrievalSource.setResource(json.get("resource_id").asText());
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

