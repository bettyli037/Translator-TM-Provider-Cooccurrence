package edu.ucdenver.ccp.cooccurrence.entities;

import javax.persistence.*;
import javax.validation.constraints.NotEmpty;
import java.util.List;

@Entity
@Table(name = "nodes")
public class Node {
    @Column(name = "id")
    @NotEmpty
    @Id
    private int id;

    @Column(name = "curie")
    @NotEmpty
    private String curie;

    @ManyToMany
    @JoinTable(name = "node_document",
            joinColumns = @JoinColumn(name = "node_id", referencedColumnName = "id"),
            inverseJoinColumns = @JoinColumn(name = "document_id", referencedColumnName = "id"))
    private List<Document> documents;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurie() {
        return curie;
    }

    public void setCurie(String curie) {
        this.curie = curie;
    }

    public List<Document> getDocuments() {
        return documents;
    }
}
