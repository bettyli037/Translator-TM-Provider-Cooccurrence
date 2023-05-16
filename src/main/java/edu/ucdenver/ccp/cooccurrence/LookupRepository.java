package edu.ucdenver.ccp.cooccurrence;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

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

    @Cacheable("categoriesforcuries")
    public Map<String, List<String>> getCategoriesForCuries(List<String> curies) {
        String query = "SELECT n.curie, nc.category " +
                "FROM nodes n INNER JOIN node_category nc ON nc.node_id = n.id " +
                "WHERE n.curie IN (:curies)";
        List<Object[]> results = new ArrayList<>();
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
        Map<String, List<String>> categoryMap = new HashMap<>();
        for (Object[] resultRow : results) {
            String curie = (String) resultRow[0];
            String category = (String) resultRow[1];
            List<String> categories;
            if (categoryMap.containsKey(curie)) {
                categories = categoryMap.get(curie);
            } else {
                categories = new ArrayList<>();
            }
            categories.add(category);
            categoryMap.put(curie, categories);
        }
        return categoryMap;
    }

    @Cacheable("labelsforcuries")
    public Map<String, String> getLabels(List<String> curies) {
        String query = "SELECT curie, label " +
                "FROM labels " +
                "WHERE curie IN (:curies)";
        List<Object[]> results = new ArrayList<>();
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
        Map<String, String> labelMap = new HashMap<>();
        for (Object[] resultRow : results) {
            String curie = (String) resultRow[0];
            String label = (String) resultRow[1];
            labelMap.put(curie, label);
        }
        return labelMap;
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
        List<Object[]> results = session.createNativeQuery(activeQuery).setParameter("p1", node1List).setParameter("p2", node2List).getResultList();
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
        Map<String, List<List<Integer>>> cooccurrences = new HashMap<>(4);
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
        List<Object[]> abstractResults = getCooccurrencesByParts(abstractQuery, concept1List, concept2List);
        List<Object[]> titleResults = getCooccurrencesByParts(titleQuery, concept1List, concept2List);
        List<Object[]> articleResults = getCooccurrencesByParts(articleQuery, concept1List, concept2List);
        List<Object[]> sentenceResults = getCooccurrencesByParts(sentenceQuery, concept1List, concept2List);
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
    public Map<String, Integer> getConceptCounts() {
        List<Object[]> results = session.createNativeQuery("SELECT document_part, COUNT(DISTINCT(curie)) " +
                "FROM concept_counts " +
                "GROUP BY document_part").getResultList();
        Map<String, Integer> countMap = new HashMap<>();
        results.forEach(row -> countMap.put((String) row[0], ((BigInteger) row[1]).intValue()));
        return countMap;
    }

    // The concept_synonyms table contains zero-to-many "equivalent identifiers" from SRI's Node Normalizer for every identifier used in Text Mined Cooccurrence
    // (zero because some don't have an entry in NN). By convention the column curie1 contains the curies used by nodes identified by Text Mining and curie2
    // contains the equivalent identifiers. There are ~20k equivalent identifiers that map to more than one TM curie, so this method returns a list of TM curies
    // for every curie provided.
    public Map<String, List<String>> getTextMinedCuriesMap(List<String> curies) {
        List<Object[]> results = session.createNativeQuery(
                "SELECT curie1, curie2 " +
                "FROM concept_synonyms " +
                "WHERE curie2 IN (:curies)")
                .setParameter("curies", curies)
                .getResultList();
        Map<String, List<String>> synonymMap = new HashMap<>(curies.size());
        for (Object[] row : results) {
            String tmCurie = (String) row[0];
            String synonymCurie = (String) row[1];
            List<String> tmCurieList;
            if (synonymMap.containsKey(synonymCurie)) {
                tmCurieList = synonymMap.get(synonymCurie);
            } else {
                tmCurieList = new ArrayList<>();
            }
            tmCurieList.add(tmCurie);
            synonymMap.put(synonymCurie, tmCurieList);
        }
        return synonymMap;
    }

    // This is similar to getTextMinedCuriesMap in design and purpose. In cases where it is not important to know which input curie matches which TM curie this
    // method avoids the extra steps of pulling every curie into a list.
    public List<String> getTextMinedCuriesList(List<String> curies) {
        return session.createNativeQuery(
                        "SELECT curie1 " +
                                "FROM concept_synonyms " +
                                "WHERE curie2 IN (:curies)")
                .setParameter("curies", curies)
                .getResultList();
    }

    @Cacheable("node_curies")
    public List<String> getTextMinedCuries() {
        return session.createNativeQuery("SELECT curie FROM nodes").getResultList();
    }

    @Transactional
    public void addSynonyms(List<List<String>> synonymsList) {
        StringBuilder insertBuilder = new StringBuilder("INSERT INTO concept_synonyms VALUES ");
        for (int i = 0; i < synonymsList.size(); i++) {
            insertBuilder.append(String.format("(:a%d, :b%d)", i, i));
            if (i < synonymsList.size() - 1) {
                insertBuilder.append(",");
            }
        }
        Query insertQuery = session.createNativeQuery(insertBuilder.toString());
        for (int i = 0; i < synonymsList.size(); i++) {
            List<String> synonymPair = synonymsList.get(i);
            insertQuery.setParameter("a" + i, synonymPair.get(0));
            insertQuery.setParameter("b" + i, synonymPair.get(1));
        }
        insertQuery.executeUpdate();
    }
}