package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

public class AttributeConstraint {

    public static final List<String> supportedAttributes = List.of(
            "biolink:concept_count_subject", "biolink:concept_count_object", "biolink:tmkp_concept_pair_count", "biolink:tmkp_normalized_google_distance",
            "biolink:tmkp_pointwise_mutual_information", "biolink:tmkp_normalized_pointwise_mutual_information", "biolink:tmkp_mutual_dependence",
            "biolink:tmkp_normalized_pointwise_mutual_information_max_denominator", "biolink:tmkp_log_frequency_biased_mutual_dependence",
            "biolink:tmkp_document_part");
    private String id;
    private String name;
    private boolean not;
    private String operator;
    private JsonNode value;
    private String unitId;
    private String unitName;
    private boolean supported = true;

    public AttributeConstraint() {
        this.unitId = null;
        this.unitName = null;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isNot() {
        return not;
    }

    public void setNot(boolean not) {
        this.not = not;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public JsonNode getValue() {
        return value;
    }

    public void setValue(JsonNode value) {
        this.value = value;
    }

    public String getUnitId() {
        return unitId;
    }

    public void setUnitId(String unitId) {
        this.unitId = unitId;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public void setSupported(boolean isSupported) {
        this.supported = isSupported;
    }

    public boolean isSupported() {
        return this.supported;
    }

    public JsonNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode constraintNode = om.createObjectNode();
        constraintNode.put("id", this.getId());
        constraintNode.put("name", this.getName());
        constraintNode.put("operator", this.getOperator());
        constraintNode.set("value", this.getValue());

        if (this.getUnitId() != null) {
            constraintNode.put("unit_id", this.getUnitId());
        }
        if (this.getUnitName() != null) {
            constraintNode.put("unit_name", this.getUnitName());
        }
        if (this.isNot()) {
            constraintNode.put("not", true);
        }
        return constraintNode;
    }

    public static AttributeConstraint parseJSON(JsonNode jsonConstraint) {
        AttributeConstraint constraint = new AttributeConstraint();
        if (!jsonConstraint.hasNonNull("id") || !jsonConstraint.hasNonNull("name") ||
                !jsonConstraint.hasNonNull("operator") || !jsonConstraint.hasNonNull("value")) {
            return null;
        }
        constraint.setId(jsonConstraint.get("id").asText());
        constraint.setName(jsonConstraint.get("name").asText());
        constraint.setOperator(jsonConstraint.get("operator").asText());
        constraint.setValue(jsonConstraint.get("value"));

        if (jsonConstraint.hasNonNull("not")) {
            constraint.setNot(jsonConstraint.get("not").asBoolean());
        }
        if(jsonConstraint.hasNonNull("unit_id")) {
            constraint.setUnitId(jsonConstraint.get("unit_id").asText());
        }
        if(jsonConstraint.hasNonNull("unit_name")) {
            constraint.setUnitId(jsonConstraint.get("unit_name").asText());
        }
        constraint.setSupported(supportedAttributes.contains(constraint.getId()));
        return constraint;
    }

}
