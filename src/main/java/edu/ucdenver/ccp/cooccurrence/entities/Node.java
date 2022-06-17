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

    @ManyToMany
    @JoinTable(name = "concept_abstract",
            joinColumns = @JoinColumn(name = "concept_id", referencedColumnName = "id"),
            inverseJoinColumns = @JoinColumn(name = "abstract_id", referencedColumnName = "id"))
    private List<Abstract> abstracts;

    @ManyToMany
    @JoinTable(name = "concept_sentence",
            joinColumns = @JoinColumn(name = "concept_id", referencedColumnName = "id"),
            inverseJoinColumns = @JoinColumn(name = "sentence_id", referencedColumnName = "id"))
    private List<Sentence> sentences;

    @ManyToMany
    @JoinTable(name = "concept_title",
            joinColumns = @JoinColumn(name = "concept_id", referencedColumnName = "id"),
            inverseJoinColumns = @JoinColumn(name = "title_id", referencedColumnName = "id"))
    private List<Title> titles;

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

    public List<Abstract> getAbstracts() {
        return abstracts;
    }

    public List<Sentence> getSentences() {
        return sentences;
    }

    public List<Title> getTitles() {
        return titles;
    }
}
