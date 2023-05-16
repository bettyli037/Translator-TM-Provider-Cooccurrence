package edu.ucdenver.ccp.cooccurrence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.ChannelOption;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.netty.http.client.HttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@Service
public class NodeNormalizerService {

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public NodeNormalizerService(WebClient.Builder webClientBuilder, @Value("${sri.url}") String endpoint) {
        HttpClient httpClient = HttpClient.create().option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
        webClient = webClientBuilder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .baseUrl(endpoint)
                .build();
        objectMapper = new ObjectMapper();
    }

    public JsonNode getNormalizedNodesInBatches(List<String> curies, int batchSize) {
        if (curies.size() == 0) {
            return objectMapper.createObjectNode();
        }
        if (curies.size() < batchSize) {
            return getNormalizedNodes(curies);
        }
        ObjectNode totalResults = objectMapper.createObjectNode();
        for (int i = 0; i < curies.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, curies.size());
            List<String> curiesSubList = curies.subList(i, endIndex);
            JsonNode partialResults = getNormalizedNodes(curiesSubList);
            Iterator<String> keyIterator = partialResults.fieldNames();
            while(keyIterator.hasNext()) {
                String key = keyIterator.next();
                JsonNode property = partialResults.get(key);
                totalResults.set(key, property);
            }
        }
        return totalResults;
    }

    public JsonNode getNormalizedNodes(List<String> curies) {
        if (curies.size() == 0) {
            return objectMapper.createObjectNode();
        }
        try {
            ObjectNode requestNode = objectMapper.createObjectNode();
            requestNode.put("conflate", true);
            requestNode.set("curies", objectMapper.convertValue(curies, ArrayNode.class));
            return webClient
                    .post()
                    .uri("/get_normalized_nodes")
                    .bodyValue(requestNode)
                    .retrieve()
                    .bodyToMono(ObjectNode.class)
                    .block();
        } catch (WebClientRequestException wex) {
            System.out.println("Request issue");
            return objectMapper.createObjectNode();
        } catch (WebClientResponseException wex) {
            System.out.println("Response issue");
            return objectMapper.createObjectNode();
        }
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

    public List<String> getNodeSynonyms(String curie, JsonNode normalizedNodes) {
        if (normalizedNodes.hasNonNull(curie)) {
            JsonNode node = normalizedNodes.get(curie);
            if (node.hasNonNull("equivalent_identifiers") && node.get("equivalent_identifiers").isArray()) {
                List<String> synonyms = new ArrayList<>();
                for (JsonNode synonymNode : node.get("equivalent_identifiers")) {
                    if (synonymNode.hasNonNull("identifier")) {
                        synonyms.add(synonymNode.get("identifier").asText());
                    }
                }
                return synonyms;
            }
        }
        return Collections.emptyList();
    }
}
