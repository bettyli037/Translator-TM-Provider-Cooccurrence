package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Qualifier {
    private String typeId;
    private String value;

    public Qualifier() {
    }

    public String getTypeId() {
        return typeId;
    }

    public void setTypeId(String typeId) {
        this.typeId = typeId;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public JsonNode toJSON() {
        return (new ObjectMapper()).convertValue(this, ObjectNode.class);
    }

    public static Qualifier parseJSON(JsonNode qualifierNode) {
        if (!qualifierNode.hasNonNull("qualifier_type_id") || !qualifierNode.hasNonNull("qualifier_value")) {
            return null;
        }
        Qualifier qualifier = new Qualifier();
        qualifier.setTypeId(qualifierNode.get("qualifier_type_id").asText());
        qualifier.setValue(qualifierNode.get("qualifier_value").asText());
        return qualifier;
    }
}
