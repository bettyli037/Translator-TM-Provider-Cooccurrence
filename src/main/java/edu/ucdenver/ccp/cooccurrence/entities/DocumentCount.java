package edu.ucdenver.ccp.cooccurrence.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotEmpty;

@Entity
@Table(name = "document_counts")
public class DocumentCount {

    @Column(name = "document_part")
    @NotEmpty
    @Id
    private String part;

    @Column(name = "record_count")
    private int recordCount;

    public String getPart() {
        return part;
    }

    public void setPart(String part) {
        this.part = part;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(int count) {
        this.recordCount = count;
    }
}
