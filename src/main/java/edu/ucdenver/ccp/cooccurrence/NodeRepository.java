package edu.ucdenver.ccp.cooccurrence;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;


public interface NodeRepository extends Repository<Node, Integer> {

    @Cacheable("concepts")
    @Query("SELECT COUNT(1) FROM Node n")
    @Transactional(readOnly = true)
    int getTotalConceptCount();

    @Cacheable("documents")
    @Query("SELECT COUNT(1) FROM Document d")
    @Transactional(readOnly = true)
    int getTotalDocumentCount();

    @Cacheable("singleConcept")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.documents d WHERE n.curie IN (:curie) AND d.part = :part")
    @Transactional(readOnly = true)
    int getSingleConceptCount(@Param("curie") String curie, @Param("part") String part);

    @Cacheable("conceptPair")
    @Query("SELECT COUNT(1) FROM Node n INNER JOIN n.documents d INNER JOIN d.nodes n2 WHERE n.curie IN (:curie1) AND n2.curie IN (:curie2) AND d.part = :part")
    @Transactional(readOnly = true)
    int getPairConceptCount(@Param("curie1") String curie1, @Param("curie2") String curie2, @Param("part") String part);

    @Query("SELECT n FROM Node n")
    @Transactional(readOnly = true)
    Page<Node> findAll(Pageable pageable);

    @Query("SELECT n FROM Node n WHERE n.curie = :curie")
    @Transactional(readOnly = true)
    Node findByCurie(@Param("curie") String curie);
}
