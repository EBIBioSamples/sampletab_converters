package uk.ac.ebi.fgpt.sampletab.subs;

import java.util.Date;

public class Event {
    private Integer id = null;
    private Integer experiment = null;
    private String eventType = null;
    private Integer wasSuccessful = null;
    private Date startTime = null;
    private Date endTime = null;
    private String machine = null;
    private String operator = null;
    private String logFile = null;
    private String comment = null;
    private int isDeleted = 0;
    
    public Integer getId() {
        return id;
    }
    public void setId(Integer id) {
        this.id = id;
    }
    public Integer getExperiment() {
        return experiment;
    }
    public void setExperiment(Integer experiment) {
        this.experiment = experiment;
    }
    public String getEventType() {
        return eventType;
    }
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
    public Integer getWasSuccessful() {
        return wasSuccessful;
    }
    public void setWasSuccessful(Integer wasSuccessful) {
        this.wasSuccessful = wasSuccessful;
    }
    public Date getStartTime() {
        return startTime;
    }
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }
    public Date getEndTime() {
        return endTime;
    }
    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
    public String getMachine() {
        return machine;
    }
    public void setMachine(String machine) {
        this.machine = machine;
    }
    public String getOperator() {
        return operator;
    }
    public void setOperator(String operator) {
        this.operator = operator;
    }
    public String getLogFile() {
        return logFile;
    }
    public void setLogFile(String logFile) {
        this.logFile = logFile;
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
}
