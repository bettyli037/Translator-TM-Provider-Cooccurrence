package edu.ucdenver.ccp.cooccurrence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Metrics {

    private final double normalizedGoogleDistance;

    private final double pointwiseMutualInformation;

    private final double normalizedPointwiseMutualInformation;

    private final double mutualDependence;

    public Metrics(int singleCount1, int singleCount2, int pairCount, int conceptCount, int documentCount) {
        normalizedGoogleDistance = calculateNormalizedGoogleDistance(singleCount1, singleCount2, pairCount, conceptCount);
        pointwiseMutualInformation = calculatePointwiseMutualInformation(singleCount1, singleCount2, pairCount, documentCount);
        normalizedPointwiseMutualInformation = calculateNormalizedPointwiseMutualInformation(singleCount1, singleCount2, pairCount, documentCount);
        mutualDependence = calculateMutualDependence(singleCount1, singleCount2, pairCount, documentCount);
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

    public ObjectNode toJSON() {
        ObjectMapper om = new ObjectMapper();
        ObjectNode metricsNode = om.createObjectNode();
        metricsNode.put("NGD", normalizedGoogleDistance);
        metricsNode.put("PMI", pointwiseMutualInformation);
        metricsNode.put("NPMI", normalizedPointwiseMutualInformation);
        metricsNode.put("MD", mutualDependence);
        return metricsNode;
    }
}
