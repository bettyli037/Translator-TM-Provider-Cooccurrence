package edu.ucdenver.ccp.cooccurrence;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Objects;

@Entity
@IdClass(NodeMetadataPK.class)
@Table(name = "node_metadata")
public class NodeMetadata {
    @Id
    private String idPrefix;

    @Id
    private String category;

    public String getIdPrefix() {
        return idPrefix;
    }

    public void setIdPrefix(String idPrefix) {
        this.idPrefix = idPrefix;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }
}

class NodeMetadataPK implements Serializable {

    String idPrefix;

    String category;

    public boolean equals(Object other) {
        if (other instanceof NodeMetadataPK) {
            NodeMetadataPK otherPK = (NodeMetadataPK) other;
            return Objects.equals(this.idPrefix, otherPK.idPrefix) &&
                    Objects.equals(this.category, otherPK.category);
        }
        return false;
    }
}
