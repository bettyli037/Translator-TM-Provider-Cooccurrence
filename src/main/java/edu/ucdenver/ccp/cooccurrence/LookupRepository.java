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
        CooccurrenceController.logger.debug(results.size() + " curies in category " + category);
        return results.stream().map(o -> (String) o).collect(Collectors.toList());
    }

    @Cacheable("cooccurrences")
    public Map<String, List<String>> getPairCounts(List<String> concept1List, List<String> concept2List) {
        if (concept1List.size() == 0 || concept2List.size() == 0) {
            return Collections.emptyMap();
        }
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
//        System.out.format("%s cooccurrences query with list sizes (%d, %d)\n",
//                documentPart, node1List.size(), node2List.size());
        if (node1List.size() == 0 || node2List.size() == 0 || !CooccurrenceController.documentParts.contains(documentPart)) {
            return Collections.emptyMap();
        }
        String abstractQuery = "" +
                "SELECT concept1_curie, concept2_curie, document_hash " +
                "FROM concept_pairs_abstract " +
                "WHERE concept1_id IN (:p1) " +
                "AND concept2_id IN (:p2) " +
                "GROUP BY concept1_curie, concept2_curie, document_hash";
        String titleQuery = "" +
                "SELECT concept1_curie, concept2_curie, document_hash " +
                "FROM concept_pairs_title " +
                "WHERE concept1_id IN (:p1) " +
                "AND concept2_id IN (:p2) " +
                "GROUP BY concept1_curie, concept2_curie, document_hash";
        String articleQuery = "" +
                "SELECT concept1_curie, concept2_curie, document_hash " +
                "FROM concept_pairs_article " +
                "WHERE concept1_id IN (:p1) " +
                "AND concept2_id IN (:p2) " +
                "GROUP BY concept1_curie, concept2_curie, document_hash";
        String sentenceQuery = "" +
                "SELECT concept1_curie, concept2_curie, document_hash " +
                "FROM concept_pairs_sentence " +
                "WHERE concept1_id IN (:p1) " +
                "AND concept2_id IN (:p2) " +
                "GROUP BY concept1_curie, concept2_curie, document_hash";
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
//        long t1 = System.currentTimeMillis();
        List<Object[]> results = session.createNativeQuery(activeQuery).setParameter("p1", node1List).setParameter("p2", node2List).getResultList();
//        long t2 = System.currentTimeMillis();
//        System.out.format("%s Query Time: %dms\n", documentPart, t2 - t1);
        Map<String, List<String>> cooccurrences = new HashMap<>();
        for (Object[] resultRow : results) {
            String key = (String) resultRow[0] + (String) resultRow[1] + documentPart;
            String value = (String) resultRow[2];
            List<String> docs;
            if (cooccurrences.containsKey(key)) {
                docs = cooccurrences.get(key);
            } else {
                docs = new ArrayList<>();
            }
            docs.add(value);
            cooccurrences.put(key, docs);
        }
//        System.out.format("Time for %s coccurrences query with list sizes (%d, %d): %dms\n",
//                documentPart, node1List.size(), node2List.size(), System.currentTimeMillis() - t1);
//        System.out.format("Found %d %s keys\n", cooccurrences.keySet().size(), documentPart);
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
                int endIndex = Math.min(i + Short.MAX_VALUE, curies.size());
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
        if (curies == null || curies.size() == 0) {
            return Collections.emptyList();
        }
        int MAX_LIST_SIZE = Short.MAX_VALUE / 2;
        List<Integer> resultsList = new ArrayList<>();
        String query = "SELECT id FROM nodes WHERE curie IN (:c)";
        for (int startIndex = 0; startIndex < curies.size(); startIndex += MAX_LIST_SIZE) {
            int endIndex = Math.min(startIndex + MAX_LIST_SIZE, curies.size());
            resultsList.addAll(session.createNativeQuery(query).setParameter("c", curies.subList(startIndex, endIndex)).getResultList());
        }
        return resultsList;
    }

    public Map<String, List<List<Integer>>> getCoccurrentNodesByParts(List<String> concept1List, List<String> concept2List) {
        if (concept1List.size() == 0 || concept2List.size() == 0) {
            return Collections.emptyMap();
        }
        int MAX_LIST_SIZE = Short.MAX_VALUE / 2;
//        System.out.format("(%d, %d)\n", concept1List.size(), concept2List.size());
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

    public List<Object[]> getCooccurrencesByParts(String query, List<Integer> concept1List, List<Integer> concept2List) {
        int MAX_LIST_SIZE = Short.MAX_VALUE / 2;
        List<Object[]> results = new ArrayList<>();
        for (int startIndex1 = 0; startIndex1 < concept1List.size(); startIndex1 += MAX_LIST_SIZE) {
            int endIndex1 = Math.min(startIndex1 + MAX_LIST_SIZE, concept1List.size());
            List<Integer> concept1Sublist = concept1List.subList(startIndex1, endIndex1);
            for (int startIndex2 = 0; startIndex2 < concept2List.size(); startIndex2 += MAX_LIST_SIZE) {
                int endIndex2 = Math.min(startIndex2 + MAX_LIST_SIZE, concept2List.size());
                List<Integer> concept2Sublist = concept2List.subList(startIndex2, endIndex2);
                // TODO: find out why the node1 -> node2 cooccurrence is not the same as node2 -> node1
                results.addAll(session.createNativeQuery(query)
                        .setParameter("p1", concept1Sublist)
                        .setParameter("p2", concept2Sublist)
                        .getResultList());
                List<Object[]> reversedResults = session.createNativeQuery(query)
                        .setParameter("p2", concept1Sublist)
                        .setParameter("p1", concept2Sublist)
                        .getResultList();
                for (Object[] row : reversedResults) {
                    results.add(new Object[]{row[1], row[0]});
                }
            }
        }
        return results;
    }

    public Map<String, List<List<Integer>>> getCooccurrentNodes(List<Integer> concept1List, List<Integer> concept2List) {
//        System.out.format("(%d, %d)\n", concept1List.size(), concept2List.size());
        Map<String, List<List<Integer>>> cooccurrences = new HashMap<>(4);
        String expansionQuery = "SELECT child_id FROM flat_node_hierarchy WHERE parent_id IN (:p)";
        String abstractQuery = "" +
                "SELECT node1, node2 AS part " +
                "FROM abstract_cooccurrences " +
                "WHERE node1 IN (:p1) AND node2 IN (:p2)";
        String titleQuery = "" +
                "SELECT node1, node2 " +
                "FROM title_cooccurrences " +
                "WHERE node1 IN (:p1) AND node2 IN (:p2)";
        String articleQuery = "" +
                "SELECT node1, node2 " +
                "FROM article_cooccurrences " +
                "WHERE node1 IN (:p1) AND node2 IN (:p2)";
        String sentenceQuery = "" +
                "SELECT node1, node2 " +
                "FROM sentence_cooccurrences " +
                "WHERE node1 IN (:p1) AND node2 IN (:p2)";
//        List<Integer> expandedList = session.createNativeQuery(expansionQuery).setParameter("p", concept2List).getResultList();
//        long t1 = System.currentTimeMillis();
        List<Object[]> abstractResults = getCooccurrencesByParts(abstractQuery, concept1List, concept2List);
//        long t2 = System.currentTimeMillis();
//        System.out.format("Abstracts (%d): %dms\n", abstractResults.size(), t2 - t1);
        List<Object[]> titleResults = getCooccurrencesByParts(titleQuery, concept1List, concept2List);
//        long t3 = System.currentTimeMillis();
//        System.out.format("Titles (%d): %dms\n", titleResults.size(), t3 - t2);
        List<Object[]> articleResults = getCooccurrencesByParts(articleQuery, concept1List, concept2List);
//        long t4 = System.currentTimeMillis();
//        System.out.format("Articles (%d): %dms\n", articleResults.size(), t4 - t3);
        List<Object[]> sentenceResults = getCooccurrencesByParts(sentenceQuery, concept1List, concept2List);
//        long t5 = System.currentTimeMillis();
//        System.out.format("Sentence (%d): %dms\n", sentenceResults.size(), t5 - t4);
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
            int endIndex = Math.min(startIndex + MAX_LIST_SIZE, parentCuries.size());
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
        long t1 = System.currentTimeMillis();
        CooccurrenceController.logger.debug(String.format("Starting getCooccurrencesByParts with (%d, %d) concepts.", concept1List.size(), concept2List.size()));
        int MAX_SUBLIST_SIZE = Short.MAX_VALUE / 2;
        Map<String, List<String>> cooccurrenceMap = new HashMap<>();
        Map<String, List<List<Integer>>> cooccurrentPairs = getCoccurrentNodesByParts(concept1List, concept2List);

        long t2 = System.currentTimeMillis();
        CooccurrenceController.logger.debug("Got cooccurrent pairs in " + (t2 - t1) + "ms");

        for (String documentPart : CooccurrenceController.documentParts) {
            if (!cooccurrentPairs.containsKey(documentPart)) {
                continue;
            }
            List<List<Integer>> pairs = cooccurrentPairs.get(documentPart);
            List<Integer> node1List = pairs.stream().map(pair -> pair.get(0)).distinct().collect(Collectors.toList());
            List<Integer> node2List = pairs.stream().map(pair -> pair.get(1)).distinct().collect(Collectors.toList());
            CooccurrenceController.logger.debug(String.format("Getting cooccurrences for part: %s with (%d, %d) concepts.", documentPart, node1List.size(), node2List.size()));
            for (int startIndex1 = 0; startIndex1 < node1List.size(); startIndex1 += MAX_SUBLIST_SIZE) {
                int endIndex1 = Math.min(startIndex1 + MAX_SUBLIST_SIZE, node1List.size());
                List<Integer> node1Sublist = node1List.subList(startIndex1, endIndex1);
                for (int startIndex2 = 0; startIndex2 < node2List.size(); startIndex2 += MAX_SUBLIST_SIZE) {
                    int endIndex2 = Math.min(startIndex2 + MAX_SUBLIST_SIZE, node2List.size());
                    List<Integer> node2Sublist = node2List.subList(startIndex2, endIndex2);
                    long t3 = System.currentTimeMillis();
                    cooccurrenceMap.putAll(getCooccurrencesByDocumentPart(node1Sublist, node2Sublist, documentPart));
                    CooccurrenceController.logger.debug(String.format("Got %s cooccurrences in %dms", documentPart, System.currentTimeMillis() - t3));
                }
            }
        }
        CooccurrenceController.logger.debug(String.format("%d total cooccurrence keys in %dms", cooccurrenceMap.keySet().size(), System.currentTimeMillis() - t2));
        return cooccurrenceMap;
    }


    public Map<String, List<String>> getDescendantHierarchy(List<String> startingConcepts) {
        if (startingConcepts == null || startingConcepts.size() == 0) {
            return Collections.emptyMap();
        }
        int MAX_LIST_SIZE = Short.MAX_VALUE;
        List<Integer> ids = getIds(startingConcepts);
        List<Object[]> resultsList = new ArrayList<>();
        String query = "" +
                "SELECT n1.curie AS parent, n2.curie AS child " +
                "FROM nodes n1 " +
                "INNER JOIN flat_node_hierarchy fnh ON fnh.parent_id = n1.id " +
                "INNER JOIN nodes n2 ON n2.id = fnh.child_id " +
                "WHERE fnh.parent_id IN (:p)";
        for (int startIndex = 0; startIndex < ids.size(); startIndex += MAX_LIST_SIZE) {
            int endIndex = Math.min(startIndex + MAX_LIST_SIZE, ids.size());
            resultsList.addAll(session.createNativeQuery(query).setParameter("p", ids.subList(startIndex, endIndex)).getResultList());
        }
        Map<String, List<String>> conceptGroups = new HashMap<>();
        for (Object[] row : resultsList) {
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