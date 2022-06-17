package edu.ucdenver.ccp.cooccurrence.entities;

import javax.persistence.*;
import javax.validation.constraints.NotEmpty;
import java.io.Serializable;
import java.util.Objects;

@Entity
@IdClass(EdgeMetadataPK.class)
@Table(name = "edge_metadata")
public class EdgeMetadata {

    @Id
    @Column
    private String subject;

    @Id
    @Column
    @NotEmpty
    private String object;

    @Id
    @Column
    @NotEmpty
    private String predicate;

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }
}

class EdgeMetadataPK implements Serializable {

    String subject;

    String object;

    String predicate;

    public boolean equals(Object other) {
        if (other instanceof EdgeMetadataPK) {
            EdgeMetadataPK otherPK = (EdgeMetadataPK) other;
            return Objects.equals(this.subject, otherPK.subject) &&
                    Objects.equals(this.object, otherPK.object) &&
                    Objects.equals(this.predicate, otherPK.predicate);
        }
        return false;
    }
}
