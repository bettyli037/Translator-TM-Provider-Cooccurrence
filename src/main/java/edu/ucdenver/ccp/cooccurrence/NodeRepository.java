package edu.ucdenver.ccp.cooccurrence;

import edu.ucdenver.ccp.cooccurrence.entities.EdgeMetadata;
import edu.ucdenver.ccp.cooccurrence.entities.Node;
import edu.ucdenver.ccp.cooccurrence.entities.NodeMetadata;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;


public interface NodeRepository extends Repository<Node, Integer> {

    @Cacheable("concepts")
    @Query("SELECT COUNT(1) FROM Node n")
    @Transactional(readOnly = true)
    int getTotalConceptCount();

    @Cacheable("documents")
    @Query("SELECT recordCount FROM DocumentCount dc WHERE dc.part = :part")
    @Transactional(readOnly = true)
    int getDocumentCount(@Param("part") String part);

    @Query("SELECT em FROM EdgeMetadata em")
    @Transactional(readOnly = true)
    List<EdgeMetadata> getEdgeMetadata();

    @Query("SELECT nm FROM NodeMetadata nm")
    @Transactional(readOnly = true)
    List<NodeMetadata> getNodeMetadata();
}
