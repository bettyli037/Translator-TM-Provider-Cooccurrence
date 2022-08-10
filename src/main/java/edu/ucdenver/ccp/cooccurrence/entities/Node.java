package edu.ucdenver.ccp.cooccurrence.entities;

import javax.persistence.*;
import javax.validation.constraints.NotEmpty;

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

}
