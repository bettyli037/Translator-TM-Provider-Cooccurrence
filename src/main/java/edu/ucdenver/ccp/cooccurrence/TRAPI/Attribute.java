package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Attribute {
    private String attributeTypeId;
    private String originalAttributeName;
    private Object value;
    private String valueTypeId;
    private String attributeSource;
    private String valueUrl;
    private String description;
    private List<Attribute> attributes;

    public Attribute() {
        originalAttributeName = null;
        valueTypeId = null;
        attributeSource = null;
        valueUrl = null;
        description = null;
        attributes = new ArrayList<>();
    }

    public String getAttributeTypeId() {
        return attributeTypeId;
    }

    public void setAttributeTypeId(String attributeTypeId) {
        this.attributeTypeId = attributeTypeId;
    }

    public String getOriginalAttributeName() {
        return originalAttributeName;
    }

    public void setOriginalAttributeName(String originalAttributeName) {
        this.originalAttributeName = originalAttributeName;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getValueTypeId() {
        return valueTypeId;
    }

    public void setValueTypeId(String valueTypeId) {
        this.valueTypeId = valueTypeId;
    }

    public String getAttributeSource() {
        return attributeSource;
    }

    public void setAttributeSource(String attributeSource) {
        this.attributeSource = attributeSource;
    }

    public String getValueUrl() {
        return valueUrl;
    }

    public void setValueUrl(String valueUrl) {
        this.valueUrl = valueUrl;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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

    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode attributeNode = om.createObjectNode();
        attributeNode.put("attribute_type_id", this.attributeTypeId);
        if (this.originalAttributeName != null) {
            attributeNode.put("original_attribute_name", this.originalAttributeName);
        }
        if (this.value instanceof String) {
            attributeNode.put("value", (String) this.value);
        } else if (this.value instanceof Integer) {
            attributeNode.put("value", (Integer) this.value);
        } else if (this.value instanceof BigInteger) {
            attributeNode.put("value", (BigInteger) this.value);
        } else if (this.value instanceof Float) {
            attributeNode.put("value", (Float) this.value);
        } else if (this.value instanceof Double) {
            attributeNode.put("value", (Double) this.value);
        } else if (this.value instanceof Boolean) {
            attributeNode.put("value", (Boolean) this.value);
        } else if (this.value instanceof List) {
            attributeNode.set("value", om.valueToTree(this.value));
        }
        if (this.valueTypeId != null) {
            attributeNode.put("value_type_id", this.valueTypeId);
        }
        if (this.attributeSource != null) {
            attributeNode.put("attribute_source", this.attributeSource);
        }
        if (this.valueUrl != null) {
            attributeNode.put("value_url", this.valueUrl);
        }
        if (this.description != null) {
            attributeNode.put("description", this.description);
        }
        if (this.attributes.size() > 0) {
            ArrayNode attributesNode = (new ObjectMapper()).createArrayNode();
            for (Attribute attribute : this.attributes) {
                attributesNode.add(attribute.toJSON());
            }
            attributeNode.set("attributes", attributesNode);
        }
        return attributeNode;
    }

    public static Attribute parseJSON(JsonNode jsonAttribute) {
        if (!jsonAttribute.hasNonNull("attribute_type_id") || !jsonAttribute.hasNonNull("value")) {
            return null;
        }
        Attribute attribute = new Attribute();
        attribute.setAttributeTypeId(jsonAttribute.get("attribute_type_id").asText());
        attribute.setValue(jsonAttribute.get("value"));
        if (jsonAttribute.hasNonNull("original_attribute_name")) {
            attribute.setOriginalAttributeName(jsonAttribute.get("original_attribute_name").asText());
        }
        if (jsonAttribute.hasNonNull("value_type_id")) {
            attribute.setValueTypeId(jsonAttribute.get("value_type_id").asText());
        }
        if (jsonAttribute.hasNonNull("attribute_source")) {
            attribute.setAttributeSource(jsonAttribute.get("attribute_source").asText());
        }
        if (jsonAttribute.hasNonNull("value_url")) {
            attribute.setValueUrl(jsonAttribute.get("value_url").asText());
        }
        if (jsonAttribute.hasNonNull("description")) {
            attribute.setDescription(jsonAttribute.get("description").asText());
        }
        if (jsonAttribute.hasNonNull("attributes") && jsonAttribute.get("attributes").isArray()) {
            Iterator<JsonNode> attributeIterator = jsonAttribute.get("attributes").elements();
            while (attributeIterator.hasNext()) {
                JsonNode attributeNode = attributeIterator.next();
                attribute.addAttribute(Attribute.parseJSON(attributeNode));
            }
        }
        return attribute;
    }
}
