package edu.ucdenver.ccp.cooccurrence.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Metrics {

    private int singleCount1;
    private int singleCount2;
    private int pairCount;
    private int conceptCount;
    private int documentCount;

    private double normalizedGoogleDistance;

    private double pointwiseMutualInformation;

    private double normalizedPointwiseMutualInformation;

    private double mutualDependence;

    private String documentPart;

    public Metrics() {
        this.singleCount1 = 0;
        this.singleCount2 = 0;
        this.pairCount = 0;
        this.conceptCount = 0;
        this.documentCount = 0;
        this.documentPart = "blank";
    }

    public Metrics(int singleCount1, int singleCount2, int pairCount, int conceptCount, int documentCount, String part) {
        this.singleCount1 = singleCount1;
        this.singleCount2 = singleCount2;
        this.pairCount = pairCount;
        this.conceptCount = conceptCount;
        this.documentCount = documentCount;
        this.documentPart = part;
        normalizedGoogleDistance = calculateNormalizedGoogleDistance(singleCount1, singleCount2, pairCount, conceptCount);
        pointwiseMutualInformation = calculatePointwiseMutualInformation(singleCount1, singleCount2, pairCount, documentCount);
        normalizedPointwiseMutualInformation = calculateNormalizedPointwiseMutualInformation(singleCount1, singleCount2, pairCount, documentCount);
        mutualDependence = calculateMutualDependence(singleCount1, singleCount2, pairCount, documentCount);
    }

    public void setSingleCount1(int singleCount1) {
        this.singleCount1 = singleCount1;
    }

    public void setSingleCount2(int singleCount2) {
        this.singleCount2 = singleCount2;
    }

    public void setPairCount(int pairCount) {
        this.pairCount = pairCount;
    }

    public void setConceptCount(int conceptCount) {
        this.conceptCount = conceptCount;
    }

    public void setDocumentCount(int documentCount) {
        this.documentCount = documentCount;
    }

    public void setDocumentPart(String part) {
        this.documentPart = part;
    }

    private static double calculateNormalizedGoogleDistance(int singleCount1, int singleCount2, int pairCount, int totalConceptCount) {
        double logFx = Math.log(singleCount1);
        double logFy = Math.log(singleCount2);
        double logFxy = Math.log(pairCount);
        double logN = Math.log(totalConceptCount);
        return (Math.max(logFx, logFy) - logFxy) / (logN - Math.min(logFx, logFy));
    }

    private static double calculatePointwiseMutualInformation(int singleCount1, int singleCount2, int pairCount, int totalDocumentCount) {
        double pxy = (double) pairCount / (double) totalDocumentCount;
        double px = (double) singleCount1 / (double) totalDocumentCount;
        double py = (double) singleCount2 / (double) totalDocumentCount;
        return Math.log(pxy / (px * py));
    }

    private static double calculateNormalizedPointwiseMutualInformation(int singleCount1, int singleCount2, int pairCount, int totalDocumentCount) {
        double pmi = calculatePointwiseMutualInformation(singleCount1, singleCount2, pairCount, totalDocumentCount);
        if (pmi == Double.NEGATIVE_INFINITY) {
            return -1.0;
        }
        double pxy = (double) pairCount / (double) totalDocumentCount;
        double offset = 0.000000001;
        double denominator = -1 * Math.log(pxy + offset);
        return pmi / denominator;
    }

    private static double calculateMutualDependence(int singleCount1, int singleCount2, int pairCount, int totalDocumentCount) {
        double pxy = (double) pairCount / (double) totalDocumentCount;
        double px = (double) singleCount1 / (double) totalDocumentCount;
        double py = (double) singleCount2 / (double) totalDocumentCount;
        return Math.log(Math.pow(pxy, 2) / (px * py));
    }

    public double getNormalizedGoogleDistance() {
        return normalizedGoogleDistance;
    }

    public double getPointwiseMutualInformation() {
        return pointwiseMutualInformation;
    }

    public double getNormalizedPointwiseMutualInformation() {
        return normalizedPointwiseMutualInformation;
    }

    public double getMutualDependence() {
        return mutualDependence;
    }

    public int getSingleCount1() {
        return singleCount1;
    }

    public int getSingleCount2() {
        return singleCount2;
    }

    public int getPairCount() {
        return pairCount;
    }

    public int getConceptCount() {
        return conceptCount;
    }

    public int getDocumentCount() {
        return documentCount;
    }

    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode metricsNode = om.createObjectNode();
        metricsNode.put("NGD", normalizedGoogleDistance);
        metricsNode.put("PMI", pointwiseMutualInformation);
        metricsNode.put("NPMI", normalizedPointwiseMutualInformation);
        metricsNode.put("MD", mutualDependence);
        return metricsNode;
    }

    public ArrayNode toJSONArray() {
        ObjectMapper om = new ObjectMapper();
        ArrayNode attributesNode = om.createArrayNode();
        ObjectNode count1Node = om.createObjectNode();
        ObjectNode count2Node = om.createObjectNode();
        ObjectNode pairNode = om.createObjectNode();
        ObjectNode ngdNode = om.createObjectNode();
        ObjectNode pmiNode = om.createObjectNode();
        ObjectNode npmiNode = om.createObjectNode();
        ObjectNode mdNode = om.createObjectNode();
        count1Node.put("attribute_type_id", "biolink:tmkp_concept1_count");
        count1Node.put("value", singleCount1);
        count1Node.put("value_type_id", "SIO:000794");
        String description = String.format("The number of times concept #1 was observed to occur at the %s level in the documents that were processed", this.documentPart);
        count1Node.put("description", description);
        count1Node.put("attribute_source", "infores:text-mining-provider-cooccurrence");

        count2Node.put("attribute_type_id", "biolink:tmkp_concept2_count");
        count2Node.put("value", singleCount2);
        count2Node.put("value_type_id", "SIO:000794");
        description = String.format("The number of times concept #2 was observed to occur at the %s level in the documents that were processed", this.documentPart);
        count2Node.put("description", description);
        count2Node.put("attribute_source", "infores:text-mining-provider-cooccurrence");

        pairNode.put("attribute_type_id", "biolink:tmkp_concept_pair_count");
        pairNode.put("value", pairCount);
        pairNode.put("value_type_id", "SIO:000794");
        description = String.format("The number of times the concepts of this assertion were observed to cooccur at the %s level in the documents that were processed", this.documentPart);
        pairNode.put("description", description);
        pairNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");

        ngdNode.put("attribute_type_id", "biolink:tmkp_normalized_google_distance");
        ngdNode.put("value", normalizedGoogleDistance);
        ngdNode.put("value_type_id", "SIO:000794");
        ngdNode.put("description", "The normalized google distance score for the concepts in this assertion based on their cooccurrence in the documents that were processed");
        ngdNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");

        pmiNode.put("attribute_type_id", "biolink:tmkp_pointwise_mutual_information");
        pmiNode.put("value", pointwiseMutualInformation);
        pmiNode.put("value_type_id", "SIO:000794");
        pmiNode.put("description", "The pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed");
        pmiNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");

        npmiNode.put("attribute_type_id", "biolink:tmkp_normalized_pointwise_mutual_information");
        npmiNode.put("value", normalizedPointwiseMutualInformation);
        npmiNode.put("value_type_id", "SIO:000794");
        npmiNode.put("description", "The normalized pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed");
        npmiNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");

        mdNode.put("attribute_type_id", "biolink:tmkp_mutual_dependence");
        mdNode.put("value", mutualDependence);
        mdNode.put("value_type_id", "SIO:000794");
        mdNode.put("description", "The mutual dependence (PMI^2) score for the concepts in this assertion based on their cooccurrence in the documents that were processed");
        mdNode.put("attribute_source", "infores:text-mining-provider-cooccurrence");

        attributesNode.add(count1Node);
        attributesNode.add(count2Node);
        attributesNode.add(pairNode);
        attributesNode.add(ngdNode);
        attributesNode.add(pmiNode);
        attributesNode.add(npmiNode);
        attributesNode.add(mdNode);
        return attributesNode;
    }
}