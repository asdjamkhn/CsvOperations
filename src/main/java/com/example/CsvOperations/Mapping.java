package com.example.CsvOperations;

public class Mapping {
    private String sourceHeader;
    private String targetHeader;
    private String dataType;
    private String metadata;
    private String compare;

    public String getSourceHeader() {
        return sourceHeader;
    }

    public void setSourceHeader(String sourceHeader) {
        this.sourceHeader = sourceHeader;
    }

    public String getTargetHeader() {
        return targetHeader;
    }

    public void setTargetHeader(String targetHeader) {
        this.targetHeader = targetHeader;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public String getCompare() {
        return compare;
    }

    public void setCompare(String compare) {
        this.compare = compare;
    }
}
