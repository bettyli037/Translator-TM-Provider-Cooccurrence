package edu.ucdenver.ccp.cooccurrence.TRAPI;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class QualifierConstraint {
    private List<Qualifier> qualifierList;

    public QualifierConstraint() {
        qualifierList = new ArrayList<>();
    }

    public List<Qualifier> getQualifierList() {
        return qualifierList;
    }

    public void setQualifierList(List<Qualifier> qualifierList) {
        this.qualifierList = qualifierList;
    }

    public void addQualifier(Qualifier qualifier) {
        this.qualifierList.add(qualifier);
    }

    public JsonNode toJSON() {
        ArrayNode qualifierConstraintNode = (new ObjectMapper()).createArrayNode();
        for (Qualifier qualifier : this.qualifierList) {
            qualifierConstraintNode.add(qualifier.toJSON());
        }
        return qualifierConstraintNode;
    }

    public static QualifierConstraint parseJSON(JsonNode constraintsNode) {
        if (!constraintsNode.hasNonNull("qualifier_set") || !constraintsNode.get("qualifier_set").isArray()) {
            return null;
        }
        QualifierConstraint qualifierConstraint = new QualifierConstraint();
        Iterator<JsonNode> qualifierIterator = constraintsNode.get("qualifier_set").elements();
        while (qualifierIterator.hasNext()) {
            JsonNode qualifierNode = qualifierIterator.next();
            qualifierConstraint.addQualifier(Qualifier.parseJSON(qualifierNode));
        }
        return qualifierConstraint;
    }
}
