package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class KnowledgeNode {
    private String name;
    private List<String> categories;
    private List<Attribute> attributes;
    private String queryKey;

    public KnowledgeNode() {
        this.name = "";
        this.categories = new ArrayList<>();
        this.attributes = new ArrayList<>();
        this.queryKey = "";
    }

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

    public void addCategory(String category) {
        this.categories.add(category);
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
        if (this.categories.size() > 0) {
            nodeNode.set("categories", om.convertValue(this.categories, ArrayNode.class));
        }
        if (this.attributes.size() > 0) {
            ArrayNode attributesNode = om.createArrayNode();
            for (Attribute attribute : this.attributes) {
                attributesNode.add(attribute.toJSON());
            }
            nodeNode.set("attributes", attributesNode);
        }
        return nodeNode;
    }

    public static KnowledgeNode parseJSON(JsonNode jsonKNode) {
        KnowledgeNode node = new KnowledgeNode();
        if (jsonKNode.hasNonNull("name")) {
            node.setName(jsonKNode.get("name").asText());
        }
        if (jsonKNode.hasNonNull("categories") && jsonKNode.get("categories").isArray()) {
            Iterator<JsonNode> cats = jsonKNode.get("categories").elements();
            while (cats.hasNext()) {
                JsonNode categoryNode = cats.next();
                node.addCategory(categoryNode.asText());
            }
        }
        if (jsonKNode.hasNonNull("attributes") && jsonKNode.get("attributes").isArray()) {
            Iterator<JsonNode> attributesIterator = jsonKNode.get("attributes").elements();
            while (attributesIterator.hasNext()) {
                JsonNode attributeNode = attributesIterator.next();
                node.addAttribute(Attribute.parseJSON(attributeNode));
            }
        }
        return node;
    }
}
