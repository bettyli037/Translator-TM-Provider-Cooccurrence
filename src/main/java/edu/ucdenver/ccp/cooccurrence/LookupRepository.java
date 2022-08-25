package edu.ucdenver.ccp.cooccurrence;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Repository;

import javax.persistence.*;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class LookupRepository {
    @PersistenceContext
    EntityManager session;

    @Cacheable("curiesforcategory")
    public List<String> getCuriesForCategory(String category) {
        List<Object> results = session.createNativeQuery("" +
                        "SELECT n.curie " +
                        "FROM node_category nc INNER JOIN nodes n ON nc.node_id = n.id " +
                        "WHERE nc.category =:category")
                .setParameter("category", category)
                .getResultList();
        return results.stream().map(o -> (String) o).collect(Collectors.toList());
    }

    @Cacheable("cooccurrences")
    public Map<String, List<String>> getPairCounts(List<String> concept1List, List<String> concept2List) {
        List<Object[]> results = session.createNativeQuery("" +
                        "SELECT concept1, concept2, part, document_hash " +
                        "FROM concept_pairs " +
                        "WHERE concept1 IN (:concept1) AND concept2 IN (:concept2) " +
                        "GROUP BY concept1, concept2, part, document_hash")
                .setParameter("concept1", concept1List)
                .setParameter("concept2", concept2List)
                .getResultList();
        Map<String, List<String>> cooccurrences = new HashMap<>();
        for (Object[] resultRow : results) {
            String key = (String) resultRow[0] + (String) resultRow[1] + (String) resultRow[2];
            String value = (String) resultRow[3];
            List<String> docs;
            if (cooccurrences.containsKey(key)) {
                docs = cooccurrences.get(key);
            } else {
                docs = new ArrayList<>();
            }
            docs.add(value);
            cooccurrences.put(key, docs);
        }
        return cooccurrences;
    }


    @Cacheable("cooccurrences")
    public Map<String, List<String>> getCooccurrencesByDocumentPart(List<Integer> node1List, List<Integer> node2List, String documentPart) {
        if (node1List.size() == 0 || node2List.size() == 0 || !CooccurrenceController.documentParts.contains(documentPart)) {
            return Collections.emptyMap();
        }
        System.out.format("%s cooccurrences query with list sizes (%d, %d)\n",
                documentPart, node1List.size(), node2List.size());
        String abstractQuery = "" +
                "SELECT concept1_curie, concept2_curie, part, document_hash " +
                "FROM concept_pairs_abstract " +
                "WHERE concept1_id IN (:p1) " +
                "AND concept2_id IN (:p2) " +
                "GROUP BY concept1_curie, concept2_curie, part, document_hash";
        String titleQuery = "" +
                "SELECT concept1_curie, concept2_curie, part, document_hash " +
                "FROM concept_pairs_title " +
                "WHERE concept1_id IN (:p1) " +
                "AND concept2_id IN (:p2) " +
                "GROUP BY concept1_curie, concept2_curie, part, document_hash";
        String articleQuery = "" +
                "SELECT concept1_curie, concept2_curie, part, document_hash " +
                "FROM concept_pairs_article " +
                "WHERE concept1_id IN (:p1) " +
                "AND concept2_id IN (:p2) " +
                "GROUP BY concept1_curie, concept2_curie, part, document_hash";
        String sentenceQuery = "" +
                "SELECT concept1_curie, concept2_curie, part, document_hash " +
                "FROM concept_pairs_sentence " +
                "WHERE concept1_id IN (:p1) " +
                "AND concept2_id IN (:p2) " +
                "GROUP BY concept1_curie, concept2_curie, part, document_hash";
        String activeQuery;
        switch (documentPart) {
            case "abstract":
                activeQuery = abstractQuery;
                break;
            case "title":
                activeQuery = titleQuery;
                break;
            case "sentence":
                activeQuery = sentenceQuery;
                break;
            default:
                activeQuery = articleQuery;
        }
        long t1 = System.currentTimeMillis();
        List<Object[]> results = session.createNativeQuery(activeQuery).setParameter("p1", node1List).setParameter("p2", node2List).getResultList();
        long t2 = System.currentTimeMillis();
        System.out.format("%s Query Time: %dms\n", documentPart, t2 - t1);
        Map<String, List<String>> cooccurrences = new HashMap<>();
        for (Object[] resultRow : results) {
            String key = (String) resultRow[0] + (String) resultRow[1] + (String) resultRow[2];
            String value = (String) resultRow[3];
            List<String> docs;
            if (cooccurrences.containsKey(key)) {
                docs = cooccurrences.get(key);
            } else {
                docs = new ArrayList<>();
            }
            docs.add(value);
            cooccurrences.put(key, docs);
        }
        System.out.format("Time for %s coccurrences query with list sizes (%d, %d): %dms\n",
                documentPart, node1List.size(), node2List.size(), System.currentTimeMillis() - t1);
        return cooccurrences;
    }

    @Cacheable("singleConceptBatch")
    public Map<String, Map<String, Integer>> getSingleCounts(List<String> curies) {
        List<Object[]> results = new ArrayList<>();
        String query = "" +
                "SELECT curie, document_part, single_count " +
                "FROM concept_counts " +
                "WHERE curie IN (:curies)";
        if (curies.size() <= Short.MAX_VALUE) {
            results = session.createNativeQuery(query)
                    .setParameter("curies", curies)
                    .getResultList();
        } else {
            for (int i = 0; i < curies.size(); i += Short.MAX_VALUE) {
                int endIndex = i + Short.MAX_VALUE >= curies.size() ? curies.size() - 1 : i + Short.MAX_VALUE;
                List<String> curiesSubList = curies.subList(i, endIndex);
                results.addAll(session.createNativeQuery(query)
                        .setParameter("curies", curiesSubList)
                        .getResultList());
            }
        }
        Map<String, Map<String, Integer>> counts = new HashMap<>();
        for (Object[] resultRow : results) {
            String firstKey = (String) resultRow[0];
            String secondKey = (String) resultRow[1];
            if (!counts.containsKey(firstKey)) {
                counts.put(firstKey, new HashMap<>());
            }
            Map<String, Integer> currentMap = counts.get(firstKey);
            currentMap.put(secondKey, (Integer) resultRow[2]);
            counts.put(firstKey, currentMap);
        }
        return counts;
    }

    public List<Integer> getIds(List<String> curies) {
        String query = "SELECT id FROM nodes WHERE curie IN (:c)";
        return session.createNativeQuery(query).setParameter("c", curies).getResultList();
    }

    public Map<String, List<List<Integer>>> getCoccurrentNodesByParts(List<String> concept1List, List<String> concept2List) {
        if (concept1List.size() == 0 || concept2List.size() == 0) {
            return Collections.emptyMap();
        }
        int MAX_LIST_SIZE = Short.MAX_VALUE / 2;
        System.out.format("(%d, %d)\n%s\n", concept1List.size(), concept2List.size(), concept1List.get(0));
        Map<String, List<List<Integer>>> cooccurrences = new HashMap<>();
        for (int startIndex = 0; startIndex < concept1List.size(); startIndex += MAX_LIST_SIZE) {
            int endIndex = Math.min(startIndex + MAX_LIST_SIZE, concept1List.size());
            List<String> concept1Sublist = concept1List.subList(startIndex, endIndex);
            for (int startIndex2 = 0; startIndex2 < concept2List.size(); startIndex2 += MAX_LIST_SIZE) {
                int endIndex2 = Math.min(startIndex2 + MAX_LIST_SIZE, concept2List.size());
                List<String> concept2Sublist = concept2List.subList(startIndex2, endIndex2);
                Map<String, List<List<Integer>>> subMap = getCooccurrentNodes(getIds(concept1Sublist), getIds(concept2Sublist));

                for (String documentPart : CooccurrenceController.documentParts) {
                    List<List<Integer>> existingPairs = cooccurrences.getOrDefault(documentPart, new ArrayList<>());
                    existingPairs.addAll(subMap.getOrDefault(documentPart, new ArrayList<>()));
                    cooccurrences.put(documentPart, existingPairs);
                }
            }
        }
        return cooccurrences;
    }

    public Map<String, List<List<Integer>>> getCooccurrentNodes(List<Integer> concept1List, List<Integer> concept2List) {
        System.out.format("(%d, %d)\n", concept1List.size(), concept2List.size());
        Map<String, List<List<Integer>>> cooccurrences = new HashMap<>(4);
        String expansionQuery = "SELECT child_id FROM flat_node_hierarchy WHERE parent_id IN (:p)";
        String abstractQuery = "" +
                "SELECT node1, node2, 'abstract' AS part " +
                "FROM abstract_cooccurrences " +
                "WHERE node1 IN (:p1) AND node2 IN (:p2)";
        String titleQuery = "" +
                "SELECT node1, node2, 'title' AS part " +
                "FROM title_cooccurrences " +
                "WHERE node1 IN (:p1) AND node2 IN (:p2)";
        String articleQuery = "" +
                "SELECT node1, node2, 'article' AS part " +
                "FROM article_cooccurrences " +
                "WHERE node1 IN (:p1) AND node2 IN (:p2)";
        String sentenceQuery = "" +
                "SELECT node1, node2, 'sentence' AS part " +
                "FROM sentence_cooccurrences " +
                "WHERE node1 IN (:p1) AND node2 IN (:p2)";
        List<Integer> expandedList = session.createNativeQuery(expansionQuery).setParameter("p", concept2List).getResultList();
        long t1 = System.currentTimeMillis();
        List<Object[]> abstractResults = session.createNativeQuery(abstractQuery)
                .setParameter("p1", concept1List)
                .setParameter("p2", expandedList)
                .getResultList();
        long t2 = System.currentTimeMillis();
        System.out.format("Abstracts (%d): %dms\n", abstractResults.size(), t2 - t1);
        List<Object[]> titleResults = session.createNativeQuery(titleQuery)
                .setParameter("p1", concept1List)
                .setParameter("p2", expandedList)
                .getResultList();
        long t3 = System.currentTimeMillis();
        System.out.format("Titles (%d): %dms\n", titleResults.size(), t3 - t2);
        List<Object[]> articleResults = session.createNativeQuery(articleQuery)
                .setParameter("p1", concept1List)
                .setParameter("p2", expandedList)
                .getResultList();
        long t4 = System.currentTimeMillis();
        System.out.format("Articles (%d): %dms\n", articleResults.size(), t4 - t3);
        List<Object[]> sentenceResults = session.createNativeQuery(sentenceQuery)
                .setParameter("p1", concept1List)
                .setParameter("p2", expandedList)
                .getResultList();
        long t5 = System.currentTimeMillis();
        System.out.format("Sentence (%d): %dms\n", sentenceResults.size(), t5 - t4);
        cooccurrences.put("abstract", abstractResults.stream().map((row) -> List.of((Integer) row[0], (Integer) row[1])).collect(Collectors.toList()));
        cooccurrences.put("title", titleResults.stream().map((row) -> List.of((Integer) row[0], (Integer) row[1])).collect(Collectors.toList()));
        cooccurrences.put("article", articleResults.stream().map((row) -> List.of((Integer) row[0], (Integer) row[1])).collect(Collectors.toList()));
        cooccurrences.put("sentence", sentenceResults.stream().map((row) -> List.of((Integer) row[0], (Integer) row[1])).collect(Collectors.toList()));
        return cooccurrences;
    }

    public Map<String, Map<String, Integer>> getHierchicalCounts(List<String> parentCuries) {
        String queryString = "" +
                "SELECT curie, document_part, document_count " +
                "FROM hierarchical_concept_counts " +
                "WHERE curie IN (:p)";
        List<Object[]> results = new ArrayList<>();
        int MAX_LIST_SIZE = Short.MAX_VALUE;
        for (int startIndex = 0; startIndex < parentCuries.size(); startIndex += MAX_LIST_SIZE) {
            int endIndex = startIndex + MAX_LIST_SIZE > parentCuries.size() ? parentCuries.size() - 1 : startIndex + MAX_LIST_SIZE;
            List<String> keySublist = parentCuries.subList(startIndex, endIndex);
            results.addAll(session.createNativeQuery(queryString).setParameter("p", keySublist).getResultList());
        }
        Map<String, Map<String, Integer>> countMap = new HashMap<>();
        for (Object[] row : results) {
            String parentCurie = (String) row[0];
            String documentPart = (String) row[1];
            Integer count = (Integer) row[2];
            if (!countMap.containsKey(parentCurie)) {
                Map<String, Integer> partMap = new HashMap<>();
                partMap.put(documentPart, count);
                countMap.put(parentCurie, partMap);
            } else if (!countMap.get(parentCurie).containsKey(documentPart)) {
                countMap.get(parentCurie).put(documentPart, count);
            }
        }
        return countMap;
    }

    // returns a Map where the key is concept1_curie + concept2_curie + documentPart and the value is a list of document hashes
    public Map<String, List<String>> getCooccurrencesByParts(List<String> concept1List, List<String> concept2List) {
        int MAX_SUBLIST_SIZE = Short.MAX_VALUE / 2;
        Map<String, List<String>> cooccurrenceMap = new HashMap<>();
        Map<String, List<List<Integer>>> cooccurrentPairs = getCoccurrentNodesByParts(concept1List, concept2List);
        for (String documentPart : CooccurrenceController.documentParts) {
            if (!cooccurrentPairs.containsKey(documentPart)) {
                continue;
            }
            List<List<Integer>> pairs = cooccurrentPairs.get(documentPart);
            List<Integer> node1List = pairs.stream().map(pair -> pair.get(0)).distinct().collect(Collectors.toList());
            List<Integer> node2List = pairs.stream().map(pair -> pair.get(1)).distinct().collect(Collectors.toList());
            for (int startIndex1 = 0; startIndex1 < node1List.size(); startIndex1 += MAX_SUBLIST_SIZE) {
                int endIndex1 = startIndex1 + MAX_SUBLIST_SIZE > node1List.size() ? node1List.size() - 1 : startIndex1 + MAX_SUBLIST_SIZE;
                List<Integer> node1Sublist = node1List.subList(startIndex1, endIndex1);
                for (int startIndex2 = 0; startIndex2 < node2List.size(); startIndex2 += MAX_SUBLIST_SIZE) {
                    int endIndex2 = startIndex2 + MAX_SUBLIST_SIZE > node2List.size() ? node2List.size() - 1 : startIndex2 + MAX_SUBLIST_SIZE;
                    List<Integer> node2Sublist = node2List.subList(startIndex2, endIndex2);
                    cooccurrenceMap.putAll(getCooccurrencesByDocumentPart(node1Sublist, node2Sublist, documentPart));
                }
            }
        }
        return cooccurrenceMap;
    }


    public Map<String, List<String>> getDescendantHierarchy(List<String> startingConcepts) {
        List<Integer> ids = getIds(startingConcepts);
        String query = "" +
                "SELECT n1.curie AS parent, n2.curie AS child " +
                "FROM nodes n1 " +
                "INNER JOIN flat_node_hierarchy fnh ON fnh.parent_id = n1.id " +
                "INNER JOIN nodes n2 ON n2.id = fnh.child_id " +
                "WHERE fnh.parent_id IN (:p)";
        List<Object[]> results = session.createNativeQuery(query).setParameter("p", ids).getResultList();
        Map<String, List<String>> conceptGroups = new HashMap<>();
        for (Object[] row : results) {
            String parent = (String) row[0];
            String child = (String) row[1];
            if (!conceptGroups.containsKey(parent)) {
                List<String> children = new ArrayList<>();
                children.add(child);
                conceptGroups.put(parent, children);
            } else {
                List<String> children = conceptGroups.get(parent);
                children.add(child);
                conceptGroups.put(parent, children);
            }
        }
        return conceptGroups;
    }

    @Cacheable("concepts")
    Map<String, Integer> getConceptCounts() {
        List<Object[]> results = session.createNativeQuery("SELECT document_part, COUNT(DISTINCT(curie)) " +
                "FROM concept_counts " +
                "GROUP BY document_part").getResultList();
        Map<String, Integer> countMap = new HashMap<>();
        results.forEach(row -> countMap.put((String) row[0], ((BigInteger) row[1]).intValue()));
        return countMap;
    }
}