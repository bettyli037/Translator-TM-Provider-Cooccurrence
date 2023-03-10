package edu.ucdenver.ccp.cooccurrence.entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.ucdenver.ccp.cooccurrence.TRAPI.Attribute;

import java.util.ArrayList;
import java.util.List;

public class Metrics {

    private int singleCount1;
    private int singleCount2;
    private int pairCount;
    private int conceptCount;
    private int documentCount;

    private double normalizedGoogleDistance;

    private double pointwiseMutualInformation;

    private double normalizedPointwiseMutualInformation;

    private double normalizedPointwiseMutualInformationMaxDenom;

    private double mutualDependence;

    private double logFrequencyBiasedMutualDependence;

    private String documentPart;

    private List<String> documentIdList;

    public Metrics() {
        this.singleCount1 = 0;
        this.singleCount2 = 0;
        this.pairCount = 0;
        this.conceptCount = 0;
        this.documentCount = 0;
        this.documentPart = "blank";
        this.documentIdList = new ArrayList<>();
    }

    public Metrics(int singleCount1, int singleCount2, int pairCount, int conceptCount, int documentCount, String part) {
        this.singleCount1 = singleCount1;
        this.singleCount2 = singleCount2;
        this.pairCount = pairCount;
        this.conceptCount = conceptCount;
        this.documentCount = documentCount;
        this.documentPart = part;
        this.documentIdList = new ArrayList<>();
        normalizedGoogleDistance = calculateNormalizedGoogleDistance(singleCount1, singleCount2, pairCount, conceptCount);
        pointwiseMutualInformation = calculatePointwiseMutualInformation(singleCount1, singleCount2, pairCount, documentCount);
        normalizedPointwiseMutualInformation = calculateNormalizedPointwiseMutualInformation(singleCount1, singleCount2, pairCount, documentCount);
        normalizedPointwiseMutualInformationMaxDenom = calculateNormalizedPointwiseMutualInformationMaxDenom(singleCount1, singleCount2, pairCount, documentCount);
        mutualDependence = calculateMutualDependence(singleCount1, singleCount2, pairCount, documentCount);
        logFrequencyBiasedMutualDependence = calculateLogFrequencyBiasedMutualDependence(singleCount1, singleCount2, pairCount, documentCount);
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

    public void setDocumentIdList(List<String> list) {
        this.documentIdList = list;
    }

    private static double calculateNormalizedGoogleDistance(int singleCount1, int singleCount2, int pairCount, int totalConceptCount) {
        if (pairCount == 0 && singleCount1 > 0 && singleCount2 > 0) {
            return Double.POSITIVE_INFINITY;
        }
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

    private static double calculateNormalizedPointwiseMutualInformationMaxDenom(int singleCount1, int singleCount2, int pairCount, int totalDocumentCount) {
        double pmi = calculatePointwiseMutualInformation(singleCount1, singleCount2, pairCount, totalDocumentCount);
        if (pmi == Double.NEGATIVE_INFINITY) {
            return -1.0;
        }
        double px = (double) singleCount1 / totalDocumentCount;
        double py = (double) singleCount2 / totalDocumentCount;
        double offset = 0.000000001;
        double denominator = -1 * Math.log(Math.max(px, py) + offset);
        return pmi / denominator;
    }

    public static double calculateLogFrequencyBiasedMutualDependence(int singleCount1, int singleCount2, int pairCount, int totalDocumentCount) {
        double md = calculateMutualDependence(singleCount1, singleCount2, pairCount, totalDocumentCount);
        double pxy = (double) pairCount / (double) totalDocumentCount;

        return md + Math.log(pxy);
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

    public double getNormalizedPointwiseMutualInformationMaxDenom() {
        return normalizedPointwiseMutualInformationMaxDenom;
    }

    public double getMutualDependence() {
        return mutualDependence;
    }

    public double getLogFrequencyBiasedMutualDependence() {
        return logFrequencyBiasedMutualDependence;
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

    public List<Attribute> toAttributeList() {
        List<Attribute> attributeList = new ArrayList<>();

        Attribute primaryKnowledgeSource = new Attribute();
        primaryKnowledgeSource.setAttributeTypeId("biolink:primary_knowledge_source");
        primaryKnowledgeSource.setValue("infores:text-mining-provider-cooccurrence");
        primaryKnowledgeSource.setValueTypeId("biolink:InformationResource");
        primaryKnowledgeSource.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(primaryKnowledgeSource);

        Attribute supportingDataSource = new Attribute();
        supportingDataSource.setAttributeTypeId("biolink:supporting_data_source");
        supportingDataSource.setValue("infores:pubmed");
        supportingDataSource.setValueTypeId("biolink:InformationResource");
        supportingDataSource.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(supportingDataSource);

        Attribute documentZone = new Attribute();
        documentZone.setAttributeTypeId("biolink:supporting_text_located_in");
        documentZone.setValue(this.documentPart);
        documentZone.setValueTypeId("IAO_0000314");
        documentZone.setAttributeSource("infores:pubmed");
        attributeList.add(documentZone);

        Attribute count1 = new Attribute();
        count1.setAttributeTypeId("biolink:concept_count_subject");
        count1.setValue(singleCount1);
        count1.setValueTypeId("SIO:000794");
        String description = String.format("The number of times concept #1 was observed to occur at the %s level in the documents that were processed", this.documentPart);
        count1.setDescription(description);
        count1.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(count1);

        Attribute count2 = new Attribute();
        count2.setAttributeTypeId("biolink:concept_count_object");
        count2.setValue(singleCount2);
        count2.setValueTypeId("SIO:000794");
        description = String.format("The number of times concept #2 was observed to occur at the %s level in the documents that were processed", this.documentPart);
        count2.setDescription(description);
        count2.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(count2);

        Attribute pairCountAttribute = new Attribute();
        pairCountAttribute.setAttributeTypeId("biolink:concept_pair_count");
        pairCountAttribute.setValue(pairCount);
        pairCountAttribute.setValueTypeId("SIO:000794");
        description = String.format("The number of times the concepts of this assertion were observed to cooccur at the %s level in the documents that were processed", this.documentPart);
        pairCountAttribute.setDescription(description);
        pairCountAttribute.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(pairCountAttribute);

        Attribute ngd = new Attribute();
        ngd.setAttributeTypeId("biolink:tmkp_normalized_google_distance");
        ngd.setValue(normalizedGoogleDistance);
        ngd.setValueTypeId("SIO:000794");
        ngd.setDescription("The normalized google distance score for the concepts in this assertion based on their cooccurrence in the documents that were processed");
        ngd.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(ngd);

        Attribute pmi = new Attribute();
        pmi.setAttributeTypeId("biolink:tmkp_pointwise_mutual_information");
        pmi.setValue(pointwiseMutualInformation);
        pmi.setValueTypeId("SIO:000794");
        pmi.setDescription("The pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed");
        pmi.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(pmi);

        Attribute npmi = new Attribute();
        npmi.setAttributeTypeId("biolink:tmkp_normalized_pointwise_mutual_information");
        npmi.setValue(normalizedPointwiseMutualInformation);
        npmi.setValueTypeId("SIO:000794");
        npmi.setDescription("The normalized pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed");
        npmi.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(npmi);

        Attribute npmimd = new Attribute();
        npmimd.setAttributeTypeId("biolink:tmkp_normalized_pointwise_mutual_information_max");
        npmimd.setValue(normalizedPointwiseMutualInformationMaxDenom);
        npmimd.setValueTypeId("SIO:000794");
        npmimd.setDescription("A variant of the normalized pointwise mutual information score for the concepts in this assertion based on their cooccurrence in the documents that were processed");
        npmimd.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(npmimd);

        Attribute md = new Attribute();
        md.setAttributeTypeId("biolink:tmkp_mutual_dependence");
        md.setValue(mutualDependence);
        md.setValueTypeId("SIO:000794");
        md.setDescription("The mutual dependence (PMI^2) score for the concepts in this assertion based on their cooccurrence in the documents that were processed");
        md.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(md);

        Attribute lfbmd = new Attribute();
        lfbmd.setAttributeTypeId("biolink:tmkp_log_frequency_biased_mutual_dependence");
        lfbmd.setValue(logFrequencyBiasedMutualDependence);
        lfbmd.setValueTypeId("SIO:000794");
        lfbmd.setDescription("The log frequency biased mutual dependence score for the concepts in this assertion based on their cooccurrence in the documents that were processed");
        lfbmd.setAttributeSource("infores:text-mining-provider-cooccurrence");
        attributeList.add(lfbmd);

        if (this.documentIdList.size() > 0) {
            Attribute supportingDocument = new Attribute();
            supportingDocument.setAttributeTypeId("biolink:supporting_document");
            supportingDocument.setValue(String.join("|", documentIdList.subList(0, Math.min(50, documentIdList.size()))));
            supportingDocument.setValueTypeId("biolink:Publication");
            description = String.format("The documents where the concepts of this assertion were observed to cooccur at the %s level.", this.documentPart);
            supportingDocument.setDescription(description);
            supportingDocument.setAttributeSource("infores:pubmed");
            attributeList.add(supportingDocument);
        }

        return attributeList;
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