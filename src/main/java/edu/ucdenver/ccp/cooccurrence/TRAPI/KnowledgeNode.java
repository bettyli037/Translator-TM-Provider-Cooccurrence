package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

public class KnowledgeNode {
    private String name;
    private List<String> categories;
    private List<String> attributes;

    private String queryKey;

    public KnowledgeNode(String name, List<String> categories) {
        this.name = name;
        this.categories = categories;
        this.attributes = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    public List<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<String> attributes) {
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
        ObjectNode nodeNode = om.createObjectNode();
        nodeNode.put("name", this.name);
        if (categories.size() > 0) {
            nodeNode.set("categories", om.convertValue(this.categories, ArrayNode.class));
        }
        if (attributes.size() > 0) {
            nodeNode.set("constraints", om.convertValue(this.attributes, ArrayNode.class));
        }
        return nodeNode;
    }
}
