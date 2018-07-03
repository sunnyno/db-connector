package com.dzytsuik.dbconnector.entity;

public enum QueryType {
    SELECT("select"), INSERT("insert"), UPDATE("update"), DELETE("delete"), CREATE_TABLE("create_table"), CREATE_DATABASE("create_database"),
    DROP_TABLE("drop_table"), DROP_DATABASE("drop_database"), ERROR("error");


    private String name;

    QueryType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
