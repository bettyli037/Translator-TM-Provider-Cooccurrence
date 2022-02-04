package edu.ucdenver.ccp.cooccurrence;

import javax.persistence.*;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Entity
@Table(name = "concept_hierarchy")
public class ConceptHierarchy {

    @Column(name = "id")
    @NotEmpty
    @Id
    private int id;

    @Column(name = "parent_curie")
    private String parentCurie;

    @Column(name = "child_curie")
    @NotNull
    private String childCurie;

    public String getParentCurie() {
        return parentCurie;
    }

    public void setParentCurie(String parentCurie) {
        this.parentCurie = parentCurie;
    }

    public String getChildCurie() {
        return childCurie;
    }

    public void setChildCurie(String childCurie) {
        this.childCurie = childCurie;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
