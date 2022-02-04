package edu.ucdenver.ccp.cooccurrence;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collection;
import java.util.List;


public interface NodeRepository extends Repository<Node, Integer> {

    @Cacheable("concepts")
    @Query("SELECT COUNT(1) FROM Node n")
    @Transactional(readOnly = true)
    int getTotalConceptCount();

    @Cacheable("documents")
    @Query("SELECT COUNT(1) FROM Document d")
    @Transactional(readOnly = true)
    int getTotalDocumentCount();

    @Cacheable("abstracts")
    @Query("SELECT COUNT(1) FROM Abstract a")
    @Transactional(readOnly = true)
    int getTotalAbstractCount();

    @Cacheable("titles")
    @Query("SELECT COUNT(1) FROM Title t")
    @Transactional(readOnly = true)
    int getTotalTitleCount();

    @Cacheable("sentences")
    @Query("SELECT COUNT(1) FROM Sentence s")
    @Transactional(readOnly = true)
    int getTotalSentenceCount();

    @Cacheable("singleConcept")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.documents d WHERE n.curie IN :curie AND d.part = :part")
    @Transactional(readOnly = true)
    int getSingleConceptCount(@Param("curie") String curie, @Param("part") String part);

    @Cacheable("singleConceptHierarchy")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.documents d WHERE n.curie IN :curies AND d.part = :part")
    @Transactional(readOnly = true)
    int getSingleConceptHierarchyCount(@Param("curies") Collection<String> curies, @Param("part") String part);

    @Cacheable("schAbstract")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.abstracts a WHERE n.curie IN :curies")
    @Transactional(readOnly = true)
    int getSingleConceptHierarchyAbstractCount(@Param("curies") Collection<String> curies);

    @Cacheable("schSentence")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.sentences a WHERE n.curie IN :curies")
    @Transactional(readOnly = true)
    int getSingleConceptHierarchySentenceCount(@Param("curies") Collection<String> curies);

    @Cacheable("schTitle")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.titles a WHERE n.curie IN :curies")
    @Transactional(readOnly = true)
    int getSingleConceptHierarchyTitleCount(@Param("curies") Collection<String> curies);

    @Cacheable("conceptPair")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.documents d INNER JOIN d.nodes n2 WHERE n.curie IN :curie1 AND n2.curie IN :curie2 AND d.part = :part")
    @Transactional(readOnly = true)
    int getPairConceptCount(@Param("curie1") String curie1, @Param("curie2") String curie2, @Param("part") String part);

    @Cacheable("conceptPairHierarchy")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.documents d INNER JOIN d.nodes n2 WHERE n.curie IN :curieList1 AND n2.curie IN :curieList2 AND d.part = :part")
    @Transactional(readOnly = true)
    int getPairConceptHierarchyCount(@Param("curieList1") Collection<String> curieList1, @Param("curieList2") Collection<String> curieList2, @Param("part") String part);

    @Cacheable("cphAbstract")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.abstracts a INNER JOIN a.nodes n2 WHERE n.curie IN :curieList1 AND n2.curie IN :curieList2")
    @Transactional(readOnly = true)
    int getPairConceptHierarchyAbstractCount(@Param("curieList1") Collection<String> curieList1, @Param("curieList2") Collection<String> curieList2);

    @Cacheable("cphSentence")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.sentences s INNER JOIN s.nodes n2 WHERE n.curie IN :curieList1 AND n2.curie IN :curieList2")
    @Transactional(readOnly = true)
    int getPairConceptHierarchySentenceCount(@Param("curieList1") Collection<String> curieList1, @Param("curieList2") Collection<String> curieList2);

    @Cacheable("cphTitle")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.titles t INNER JOIN t.nodes n2 WHERE n.curie IN :curieList1 AND n2.curie IN :curieList2")
    @Transactional(readOnly = true)
    int getPairConceptHierarchyTitleCount(@Param("curieList1") Collection<String> curieList1, @Param("curieList2") Collection<String> curieList2);

    @Query("SELECT n FROM Node n")
    @Transactional(readOnly = true)
    Page<Node> findAll(Pageable pageable);

    @Query("SELECT n FROM Node n WHERE n.curie = :curie")
    @Transactional(readOnly = true)
    Node findByCurie(@Param("curie") String curie);

    @Query("SELECT parentCurie FROM ConceptHierarchy WHERE childCurie IN :curies")
    @Transactional(readOnly = true)
    List<String> getConceptAncestors(@Param("curies") Collection<String> curies);
}
