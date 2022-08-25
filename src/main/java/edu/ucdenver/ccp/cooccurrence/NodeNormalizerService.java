package edu.ucdenver.ccp.cooccurrence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.IOException;
import java.util.List;

@Service
public class NodeNormalizerService {

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public NodeNormalizerService(WebClient.Builder webClientBuilder, @Value("${sri.url}") String endpoint) {
        webClient = webClientBuilder.baseUrl(endpoint).build();
        objectMapper = new ObjectMapper();
    }

    public JsonNode getNormalizedNodes(List<String> curies) {
        if (curies.size() == 0) {
            return objectMapper.createObjectNode();
        }
        ObjectNode requestNode = objectMapper.createObjectNode();
        requestNode.put("conflate", false);
        requestNode.set("curies", objectMapper.convertValue(curies, ArrayNode.class));
        return webClient
                .post()
                .uri("/get_normalized_nodes")
                .bodyValue(requestNode)
                .retrieve()
                .bodyToMono(ObjectNode.class)
                .block();
    }

    public String getNodeName(String curie, JsonNode normalizedNodes) {
        if (normalizedNodes.hasNonNull(curie)) {
            JsonNode idNode = normalizedNodes.get(curie).get("id");
            if (!idNode.hasNonNull("label")) {
                return idNode.get("identifier").asText();
            }
            return idNode.get("label").textValue();
        }
        return curie;
    }

    public List<String> getNodeCategories(String curie, JsonNode normalizedNodes) {
        try {
            if (normalizedNodes.hasNonNull(curie)) {
                JsonNode typeNode = normalizedNodes.get(curie).get("type");
                return objectMapper.readerForListOf(String.class).readValue(typeNode);
            }
        } catch (IOException iex) {
            System.out.println(iex.getLocalizedMessage());
        }
        return List.of("biolink:Entity");
    }
}
