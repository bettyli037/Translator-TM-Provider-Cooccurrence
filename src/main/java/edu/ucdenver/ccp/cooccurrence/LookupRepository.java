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
        return results.stream().map(o -> (String) o).collect(Collectors.toList());
    }

    @Cacheable("cooccurrences")
    public Map<String, List<String>> getCooccurrences(List<String> concept1List, List<String> concept2List) {
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

    @Cacheable("singleConceptBatch")
    public Map<String, Map<String, Integer>> getSingleCounts(List<String> curies) {
        List<Object[]> results = session.createNativeQuery(
                        "SELECT curie, document_part, single_count " +
                                "FROM concept_counts " +
                                "WHERE curie IN (:curies)")
                .setParameter("curies", curies)
                .getResultList();
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

    @Transactional
    public Map<String, Map<String, BigInteger>> getHierchicalCounts(Map<String, List<String>> hierarchy, boolean includeKeys) {
        String tempTableCreateStatement = "CREATE TEMPORARY TABLE curie_hierarchy (parent_curie VARCHAR(50), child_curie VARCHAR(50)) ON COMMIT DROP";
        StringBuilder insertHierarchyStatement = new StringBuilder("INSERT INTO curie_hierarchy");
        insertHierarchyStatement.append(" (parent_curie, child_curie) VALUES ");
        List<String> keyList = new ArrayList<>(hierarchy.keySet());
        List<String> values = new ArrayList<>();
        for (int i = 0; i < keyList.size(); i++) {
            List<String> childList = hierarchy.get(keyList.get(i));
            if (includeKeys) {
                values.add(String.format("(:p%d, :p%d)", i, i));
            }
            for (int j = 0; j < childList.size(); j++) {
                values.add(String.format("(:p%d, :ca%db%d)", i, i, j));
            }
        }
        insertHierarchyStatement.append(String.join(",", values));
        String abstractsQuery = "" +
                "SELECT parent_curie, 'abstract' AS document_part, COUNT(DISTINCT(hash)) " +
                "FROM curie_hierarchy ch " +
                "INNER JOIN nodes n ON n.curie = ch.child_curie " +
                "INNER JOIN node_abstract na ON na.node_id = n.id " +
                "INNER JOIN abstracts a ON a.id = na.abstract_id " +
                "GROUP BY parent_curie";
        String titlesQuery = "" +
                "SELECT parent_curie, 'title' AS document_part, COUNT(DISTINCT(hash)) " +
                "FROM curie_hierarchy ch " +
                "INNER JOIN nodes n ON n.curie = ch.child_curie " +
                "INNER JOIN node_title nt ON nt.node_id = n.id " +
                "INNER JOIN titles t ON t.id = nt.title_id " +
                "GROUP BY parent_curie";
        String sentencesQuery = "" +
                "SELECT parent_curie, 'sentence' AS document_part, COUNT(DISTINCT(hash)) " +
                "FROM curie_hierarchy ch " +
                "INNER JOIN nodes n ON n.curie = ch.child_curie " +
                "INNER JOIN node_sentence ns ON ns.node_id = n.id " +
                "INNER JOIN sentences s ON s.id = ns.sentence_id " +
                "GROUP BY parent_curie";
        String articlesQuery = "" +
                "SELECT parent_curie, 'article' AS document_part, COUNT(DISTINCT(hash)) " +
                "FROM curie_hierarchy ch " +
                "INNER JOIN nodes n ON n.curie = ch.child_curie " +
                "INNER JOIN node_article na ON na.node_id = n.id " +
                "INNER JOIN articles a ON a.id = na.article_id " +
                "GROUP BY parent_curie";
        session.createNativeQuery(tempTableCreateStatement).executeUpdate();
        Query insertQuery = session.createNativeQuery(insertHierarchyStatement.toString());
        int a = 0;
        for (Map.Entry<String, List<String>> entry : hierarchy.entrySet()) {
            if (includeKeys || entry.getValue().size() > 0) {
                insertQuery.setParameter("p" + a, entry.getKey());
            }
            for (int b = 0; b < entry.getValue().size(); b++) {
                insertQuery.setParameter(String.format("ca%db%d", a, b), entry.getValue().get(b));
            }
            a++;
        }
        insertQuery.executeUpdate();
        List<Object[]> results = session.createNativeQuery(abstractsQuery).getResultList();
        results.addAll(session.createNativeQuery(titlesQuery).getResultList());
        results.addAll(session.createNativeQuery(sentencesQuery).getResultList());
        results.addAll(session.createNativeQuery(articlesQuery).getResultList());
        Map<String, Map<String, BigInteger>> countMap = new HashMap<>();
        for (Object[] row : results) {
            String parentCurie = (String) row[0];
            String documentPart = (String) row[1];
            BigInteger count = (BigInteger) row[2];
            if (!countMap.containsKey(parentCurie)) {
                Map<String, BigInteger> partMap = new HashMap<>();
                partMap.put(documentPart, count);
                countMap.put(parentCurie, partMap);
            } else if (!countMap.get(parentCurie).containsKey(documentPart)) {
                countMap.get(parentCurie).put(documentPart, count);
            }
        }
        return countMap;
    }

    public List<Object[]> getRelevantConceptChildren(Collection<String> curies) {
        return session.createNativeQuery("" +
                        "SELECT parent_curie, child_curie " +
                        "FROM concept_hierarchy " +
                        "INNER JOIN nodes n ON n.curie = child_curie " +
                        "WHERE parent_curie IN (:curies)")
                .setParameter("curies", curies)
                .getResultList();
    }

    public Map<String, List<String>> getCooccurrencesByParts(List<String> concept1List, List<String> concept2List) {
        if (concept1List.size() < 1000 && concept2List.size() < 1000) {
            return getCooccurrences(concept1List, concept2List);
        }
        Map<String, List<String>> cooccurrenceMap = new HashMap<>();
        for (int fromIndex1 = 0; fromIndex1 < concept1List.size(); fromIndex1 += 1000) {
            int toIndex1 = fromIndex1 + 1000;
            if (toIndex1 > concept1List.size()) {
                toIndex1 = concept1List.size();
            }
            for (int fromIndex2 = 0; fromIndex2 < concept2List.size(); fromIndex2 += 1000) {
                int toIndex2 = fromIndex2 + 1000;
                if (toIndex2 > concept2List.size()) {
                    toIndex2 = concept2List.size();
                }
                cooccurrenceMap.putAll(getCooccurrences(concept1List.subList(fromIndex1, toIndex1), concept2List.subList(fromIndex2, toIndex2)));
            }
        }
        return cooccurrenceMap;
    }

    public List<Object[]> getConceptChildrenByParts(List<String> curies) {
        if (curies.size() < 1000) {
            return getRelevantConceptChildren(curies);
        }
        List<Object[]> resultRows = new ArrayList<>();
        for (int fromIndex = 0; fromIndex < curies.size(); fromIndex+= 1000) {
            int toIndex = fromIndex + 1000;
            if (toIndex > curies.size()) {
                toIndex = curies.size();
            }
            resultRows.addAll(getRelevantConceptChildren(curies.subList(fromIndex, toIndex)));
        }
        return resultRows;
    }

    public Map<String, List<String>> getDescendantHierarchy(List<String> startingConcepts, Set<String> excludedConcepts) {
        Map<String, List<String>> conceptGroups = new HashMap<>();
        List<String> allChildren = new ArrayList<>();
        for (String concept : startingConcepts) {
            conceptGroups.put(concept, new ArrayList<>());
        }

        List<Object[]> group = getConceptChildrenByParts(startingConcepts.stream().distinct().collect(Collectors.toList()));
        for (Object[] obj : group) {
            String parent = (String) obj[0];
            String child = (String) obj[1];
            if (excludedConcepts.contains(child)) {
                continue;
            }
            List<String> children = conceptGroups.get(parent);
            children.add(child);
            allChildren.add(child);
            conceptGroups.put(parent, children);
        }

        if (allChildren.size() == 0) {
            // Uniquify all the descendant lists for all the parents.
            for (String key : conceptGroups.keySet()) {
                if (conceptGroups.get(key).size() > 0) {
                    List<String> uniqueChildren = conceptGroups.get(key).stream().distinct().collect(Collectors.toList());
                    conceptGroups.put(key, uniqueChildren);
                }
            }
            return conceptGroups;
        }

        Map<String, List<String>> childHierarchy = getDescendantHierarchy(allChildren, excludedConcepts);
        for (Map.Entry<String, List<String>> parentEntry : conceptGroups.entrySet()) {
            List<String> newChildList = new ArrayList<>(parentEntry.getValue());
            for (String child : parentEntry.getValue()) {
                if (childHierarchy.containsKey(child)) {
                    newChildList.addAll(childHierarchy.get(child));
                }
            }
            if (newChildList.size() != parentEntry.getValue().size()) {
                conceptGroups.put(parentEntry.getKey(), newChildList);
            }
        }

        for (String key : conceptGroups.keySet()) {
            if (conceptGroups.get(key).size() > 0) {
                List<String> uniqueChildren = conceptGroups.get(key).stream().distinct().collect(Collectors.toList());
                conceptGroups.put(key, uniqueChildren);
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