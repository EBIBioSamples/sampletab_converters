package uk.ac.ebi.fgpt.sampletab.subs;

import java.util.Date;

/**
 * This class represents an entry in the Submission Tracking database "experiments" table, 
 * and roughly corresponds to the MSI section of a SampleTab file.
 * 
 * @author Adam Faulconbridge
 *
 */
public class Experiment {
    
    private Integer id = null;
    private String accession = null;
    private String name = null;
    private Date submitted = null;
    private Date lastProcessed = null;
    private String comment = null;
    private int isDeleted = 0;
    private Integer numGroups = null;
    private Integer numSamples = null;
    private Date releaseDate = null;
    private Boolean isReleased = null;
    
    public Integer getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getAccession() {
        return accession;
    }
    public void setAccession(String accession) {
        this.accession = accession;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public Date getSubmitted() {
        return submitted;
    }
    public void setSubmitted(Date submitted) {
        this.submitted = submitted;
    }
    public Date getLastProcessed() {
        return lastProcessed;
    }
    public void setLastProcessed(Date lastProcessed) {
        this.lastProcessed = lastProcessed;
    }
    public String getComment() {
        return comment;
    }
    public void setComment(String comment) {
        this.comment = comment;
    }
    public int isDeleted() {
        return isDeleted;
    }
    public void setDeleted(int isDeleted) {
        this.isDeleted = isDeleted;
    }
    public Integer getNumGroups() {
        return numGroups;
    }
    public void setNumGroups(Integer numGroups) {
        this.numGroups = numGroups;
    }
    public Integer getNumSamples() {
        return numSamples;
    }
    public void setNumSamples(Integer numSamples) {
        this.numSamples = numSamples;
    }
    public Date getReleaseDate() {
        return releaseDate;
    }
    public void setReleaseDate(Date releaseDate) {
        this.releaseDate = releaseDate;
    }
    public Boolean isReleased() {
        return isReleased;
    }
    public void setReleased(Boolean isReleased) {
        this.isReleased = isReleased;
    }
}
