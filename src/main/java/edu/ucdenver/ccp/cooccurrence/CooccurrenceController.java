package edu.ucdenver.ccp.cooccurrence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.*;

@RestController
public class CooccurrenceController {

    private final NodeRepository nodeRepo;

    public CooccurrenceController(NodeRepository repo) {
        this.nodeRepo = repo;
    }

    @GetMapping("/concepts/count")
    public String conceptCount() {
        return String.format("%d", nodeRepo.getTotalConceptCount());
    }

    @GetMapping("/concepts")
    public String concepts() {
        Pageable pageable = PageRequest.of(0, 5);
        return String.format("%d", nodeRepo.findAll(pageable).getTotalElements());
    }

    @GetMapping("/concepts/{curie}")
    public String getConcept(@PathVariable("curie") String curie) {
        Node n = nodeRepo.findByCurie(curie);
        return String.format("{id: %d, curie: %s, document_count: %d}", n.getId(), n.getCurie(), n.getDocuments().size());
    }

    @PostMapping("/metrics")
    public String calculateMetrics(@RequestBody JsonNode request) {
        if (!(request.hasNonNull("concept1") && request.hasNonNull("concept2") && request.hasNonNull("document_part"))) {
            return "Invalid Request";
        }
        long startTime = System.currentTimeMillis();
        long splitTime1 = startTime, splitTime2;
        String curie1 = request.get("concept1").asText();
        String curie2 = request.get("concept2").asText();
        String part = request.get("document_part").asText();
        int singleCount1 = nodeRepo.getSingleConceptCount(curie1, part);
        splitTime2 = System.currentTimeMillis();
        System.out.printf("First concept count time: %d%n", splitTime2 - splitTime1);
        int singleCount2 = nodeRepo.getSingleConceptCount(curie2, part);
        splitTime1 = System.currentTimeMillis();
        System.out.printf("Second concept count time: %d%n", splitTime1 - splitTime2);
        int pairCount = nodeRepo.getPairConceptCount(curie1, curie2, part);
        splitTime2 = System.currentTimeMillis();
        System.out.printf("Pair concept count time: %d%n", splitTime2 - splitTime1);
        int totalConceptCount = nodeRepo.getTotalConceptCount();
        splitTime1 = System.currentTimeMillis();
        System.out.printf("Total concept count time: %d%n", splitTime1 - splitTime2);
        int totalDocumentCount = nodeRepo.getTotalDocumentCount();
        splitTime2 = System.currentTimeMillis();
        System.out.printf("Total document count time: %d%n", splitTime2 - splitTime1);

        ObjectMapper om = new ObjectMapper();
        ObjectNode responseNode = om.createObjectNode();
        ObjectNode basicMetricsNode = om.createObjectNode();
        responseNode.set("request", request);
        basicMetricsNode.put("single_count1", singleCount1);
        basicMetricsNode.put("single_count2", singleCount2);
        basicMetricsNode.put("pair_count", pairCount);
        basicMetricsNode.put("total_concept_count", totalConceptCount);
        basicMetricsNode.put("total_document_count", totalDocumentCount);
        responseNode.set("counts", basicMetricsNode);
        responseNode.put("NGD", normalizedGoogleDistance(singleCount1, singleCount2, pairCount, totalConceptCount));
        responseNode.put("PMI", pointwiseMutualInformation(singleCount1, singleCount2, pairCount, totalDocumentCount));
        responseNode.put("NPMI", normalizedPointwiseMutualInformation(singleCount1, singleCount2, pairCount, totalDocumentCount));
        responseNode.put("MD", mutualDependence(singleCount1, singleCount2, pairCount, totalDocumentCount));
        long endTime = System.currentTimeMillis();
        System.out.println(pointwiseMutualInformation(singleCount1, singleCount2, pairCount, totalDocumentCount));
        responseNode.put("elapsed_time", endTime - startTime);
        return responseNode.toPrettyString();
    }

    private static double normalizedGoogleDistance(int singleCount1, int singleCount2, int pairCount, int totalConceptCount) {
        double logFx = Math.log(singleCount1);
        double logFy = Math.log(singleCount2);
        double logFxy = Math.log(pairCount);
        double logN = Math.log(totalConceptCount);
        return (Math.max(logFx, logFy) - logFxy) / (logN - Math.min(logFx, logFy));
    }

    private static double pointwiseMutualInformation(int singleCount1, int singleCount2, int pairCount, int totalDocumentCount) {
        double pxy = (double) pairCount / (double) totalDocumentCount;
        double px = (double) singleCount1 / (double) totalDocumentCount;
        double py = (double) singleCount2 / (double) totalDocumentCount;
        return Math.log(pxy / (px * py));
    }

    private static double normalizedPointwiseMutualInformation(int singleCount1, int singleCount2, int pairCount, int totalDocumentCount) {
        double pmi = pointwiseMutualInformation(singleCount1, singleCount2, pairCount, totalDocumentCount);
        if (pmi == Double.NEGATIVE_INFINITY) {
            return -1.0;
        }
        double pxy = (double) pairCount / (double) totalDocumentCount;
        double offset = 0.000000001;
        double denominator = -1 * Math.log(pxy + offset);
        return pmi / denominator;
    }

    private static double mutualDependence(int singleCount1, int singleCount2, int pairCount, int totalDocumentCount) {
        double pxy = (double) pairCount / (double) totalDocumentCount;
        double px = (double) singleCount1 / (double) totalDocumentCount;
        double py = (double) singleCount2 / (double) totalDocumentCount;
        return Math.log(Math.pow(pxy, 2) / (px * py));
    }

}
