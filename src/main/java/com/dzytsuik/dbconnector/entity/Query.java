package com.dzytsuik.dbconnector.entity;


import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Query {
    private QueryType type;
    private String dataBase;
    private String table;
    private Map<String, String> data;
    private List<String> metadata;

    public QueryType getType() {
        return type;
    }

    public void setType(QueryType type) {
        this.type = type;
    }


    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Map<String, String> getData() {
        return data;
    }

    public void setData(Map<String, String> data) {
        this.data = data;
    }

    public List<String> getMetadata() {
        return metadata;
    }

    public void setMetadata(List<String> metadata) {
        this.metadata = metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Query query = (Query) o;
        return type == query.type &&
                Objects.equals(dataBase, query.dataBase) &&
                Objects.equals(table, query.table) &&
                Objects.equals(data, query.data) &&
                Objects.equals(metadata, query.metadata);
    }

    @Override
    public int hashCode() {

        return Objects.hash(type, dataBase, table, data, metadata);
    }
}
