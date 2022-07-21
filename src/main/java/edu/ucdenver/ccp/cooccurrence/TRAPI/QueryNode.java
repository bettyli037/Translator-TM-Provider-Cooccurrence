package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class QueryNode {
    private List<String> ids;
    private List<String> categories;
    private boolean isSet;
    private List<AttributeConstraint> constraints;
    private Map<String, JsonNode> additionalProperties;

    public QueryNode() {
        ids = new ArrayList<>();
        categories = new ArrayList<>();
        constraints = new ArrayList<>();
        additionalProperties = new HashMap<>();
        isSet = false;
    }

    public List<String> getIds() {
        return ids;
    }

    public void setIds(List<String> ids) {
        this.ids = ids;
    }

    public void addId(String id) {
        ids.add(id);
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    public void addCategory(String category) {
        categories.add(category);
    }

    public boolean isSet() {
        return isSet;
    }

    public void setSet(boolean set) {
        isSet = set;
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

    public Map<String, JsonNode> getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Map<String, JsonNode> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    public void addAdditionalProperty(String key, JsonNode value) {
        additionalProperties.put(key, value);
    }

    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode nodeNode = om.createObjectNode();
        if (ids.size() > 0) {
            nodeNode.set("ids", om.convertValue(ids, ArrayNode.class));
        }
        if (categories.size() > 0) {
            nodeNode.set("categories", om.convertValue(categories, ArrayNode.class));
        }
        if (constraints.size() > 0) {
            nodeNode.set("constraints", om.convertValue(constraints, ArrayNode.class));
        }
        if (isSet) {
            nodeNode.put("is_set", true);
        }
        for (Map.Entry<String, JsonNode> kv : this.additionalProperties.entrySet()) {
            nodeNode.set(kv.getKey(), kv.getValue());
        }
        return nodeNode;
    }

    public static QueryNode parseJSON(JsonNode jsonQNode) {
        // Note: the YAML definition of QNode pluralizes "ids" and "categories" but the simple.json example in ReasonerAPI uses the singular.
        if (!(jsonQNode.hasNonNull("id") || jsonQNode.hasNonNull("ids")) &&
                !(jsonQNode.hasNonNull("category") || jsonQNode.hasNonNull("categories"))) {
            return null;
        }
        QueryNode node = new QueryNode();
        if (jsonQNode.hasNonNull("is_set")) {
            node.setSet(jsonQNode.get("is_set").asBoolean());
        }
        if (jsonQNode.hasNonNull("id") || jsonQNode.hasNonNull("ids")) {
            JsonNode idNode = jsonQNode.hasNonNull("id") ? jsonQNode.get("id") : jsonQNode.get("ids");
            if (idNode.isArray()) {
                Iterator<JsonNode> ids = idNode.elements();
                while (ids.hasNext()) {
                    JsonNode innerIdNode = ids.next();
                    node.addId(innerIdNode.asText());
                }
            } else if (idNode.isValueNode()) {
                node.addId(idNode.asText());
            } else {
                return null;
            }
        }
        if (jsonQNode.hasNonNull("category") || jsonQNode.hasNonNull("categories")) {
            JsonNode categoryNode = jsonQNode.hasNonNull("category") ? jsonQNode.get("category") : jsonQNode.get("categories");
            if (categoryNode.isArray()) {
                Iterator<JsonNode> cats = categoryNode.elements();
                while (cats.hasNext()) {
                    JsonNode innerCategoryNode = cats.next();
                    node.addCategory(innerCategoryNode.asText());
                }
            } else if (categoryNode.isValueNode()) {
                node.addCategory(categoryNode.asText());
            } else {
                return null;
            }
        }

        if (jsonQNode.hasNonNull("constraints") && jsonQNode.get("constraints").isArray()) {
            JsonNode constraintsNode = jsonQNode.get("constraints");
            Iterator<JsonNode> constraintIterator = constraintsNode.elements();
            while (constraintIterator.hasNext()) {
                JsonNode constraintNode = constraintIterator.next();
                node.addConstraint(AttributeConstraint.parseJSON(constraintNode));
            }
        }

        Iterator<String> keyIterator = jsonQNode.fieldNames();
        List<String> excludedKeys = List.of("id", "ids", "category", "categories", "is_set", "constraints");
        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            if (excludedKeys.contains(key)) {
                continue;
            }
            node.addAdditionalProperty(key, jsonQNode.get(key));
        }

        return node;
    }
}
