package edu.ucdenver.ccp.cooccurrence.entities;

import javax.persistence.*;
import javax.validation.constraints.NotEmpty;
import java.util.List;

@Entity
@Table(name = "documents")
public class Document {

    @Column(name = "id")
    @NotEmpty
    @Id
    private int id;

    @Column(name = "document_hash")
    @NotEmpty
    private String hash;

    @Column(name = "document_part")
    private String part;

    @ManyToMany
    @JoinTable(name = "node_document",
            joinColumns = @JoinColumn(name = "document_id", referencedColumnName = "id"),
            inverseJoinColumns = @JoinColumn(name = "node_id", referencedColumnName = "id"))
    private List<Node> nodes;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public String getPart() {
        return part;
    }

    public void setPart(String part) {
        this.part = part;
    }

    public List<Node> getNodes() {
        return nodes;
    }
}
