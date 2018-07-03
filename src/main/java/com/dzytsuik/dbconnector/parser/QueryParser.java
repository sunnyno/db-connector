package com.dzytsuik.dbconnector.parser;

import com.dzytsuik.dbconnector.entity.Query;
import com.dzytsuik.dbconnector.entity.QueryType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParser {

    private static final String TYPE_TAG = "type";
    private static final String DATABASE_TAG = "database";
    private static final String TABLE_TAG = "table";
    private static final String DATA_TAG = "data";
    private static final String METADATA_TAG = "metadata";

    private static final String SELECT = "select";
    private static final String CREATE_DATABASE = "create database";
    private static final String CREATE_TABLE = "create table";
    private static final String UPDATE = "update";
    private static final String DELETE = "delete";
    private static final String DROP_TABLE = "drop table";
    private static final String DROP_DATABASE = "drop database";
    private static final String INSERT = "insert";

    private static final int DATABASE_INDEX = 1;
    private static final int TABLE_INDEX = 2;
    private static final int COLUMN_INDEX = 3;
    private static final int DATA_INDEX = 4;
    private static final int METADATA_INDEX = 3;


    private Query query;

    public String parseQuery(String sql) throws SQLException {

        JSONObject parsedQuery = new JSONObject();
        query = getQuery(sql.trim());
        parsedQuery.put(TYPE_TAG, query.getType().getName());
        parsedQuery.put(DATABASE_TAG, query.getDataBase());

        String table = query.getTable();
        if (table != null) {
            parsedQuery.put(TABLE_TAG, table);
        }

        Map<String, String> data = query.getData();
        if (data != null && !data.isEmpty()) {
            JSONObject dataMap = new JSONObject(data);
            parsedQuery.put(DATA_TAG, dataMap);
        }

        List<String> metadata = query.getMetadata();
        if (metadata != null && !metadata.isEmpty()) {
            JSONArray metadataArray = new JSONArray(metadata);
            parsedQuery.put(METADATA_TAG, metadataArray);
        }

        return parsedQuery.toString();
    }


    public Query getQuery() {
        return query;
    }

    Query getQuery(String sql) throws SQLException {
        Query query = new Query();
        if (sql.startsWith(SELECT)) {
            query.setType(QueryType.SELECT);
            parseSelect(sql, query);
        } else if (sql.startsWith(CREATE_DATABASE)) {
            query.setType(QueryType.CREATE_DATABASE);
            parseCreateDatabase(sql, query);
        } else if (sql.startsWith(CREATE_TABLE)) {
            query.setType(QueryType.CREATE_TABLE);
            parseCreateTable(sql, query);
        } else if (sql.startsWith(UPDATE)) {
            query.setType(QueryType.UPDATE);
            parseUpdate(sql, query);
        } else if (sql.startsWith(DELETE)) {
            query.setType(QueryType.DELETE);
            parseDelete(sql, query);
        } else if (sql.startsWith(DROP_TABLE)) {
            query.setType(QueryType.DROP_TABLE);
            parseDropTable(sql, query);
        } else if (sql.startsWith(DROP_DATABASE)) {
            query.setType(QueryType.DROP_DATABASE);
            parseDropDatabase(sql, query);
        } else if (sql.startsWith(INSERT)) {
            query.setType(QueryType.INSERT);
            parseInsert(sql, query);
        } else {
            throw new SQLException("Invalid sql");
        }

        return query;
    }


    private Map<String, String> getDataMap(String metadata, String data) {
        Map<String, String> dataMap = new HashMap<>();
        String[] splitMeta = metadata.split(",\\s");
        String[] splitData = data.split(",\\s");
        for (int i = 0; i < splitMeta.length; i++) {
            dataMap.put(splitMeta[i], splitData[i]);
        }
        return dataMap;
    }

    void parseInsert(String sql, Query query) {
        String queryPattern = "INSERT\\s+INTO\\s+(\\S+)\\.(\\S+)\\((.+)\\)\\s+VALUES\\((.+)\\)";
        Matcher matcher = getMatcher(sql, queryPattern);
        if (matcher.find()) {
            query.setDataBase(matcher.group(DATABASE_INDEX));
            query.setTable(matcher.group(TABLE_INDEX));

            String metadata = matcher.group(METADATA_INDEX);
            String data = matcher.group(DATA_INDEX);
            Map<String, String> dataMap = getDataMap(metadata, data);
            query.setData(dataMap);

            query.setMetadata(Arrays.asList(metadata.split(",\\s")));
        }
    }

    void parseDropDatabase(String sql, Query query) {
        String queryPattern = "DROP\\s+DATABASE\\s+(\\S+)";
        Matcher matcher = getMatcher(sql, queryPattern);
        if (matcher.find()) {
            query.setDataBase(matcher.group(DATABASE_INDEX));
        }
    }


    void parseDropTable(String sql, Query query) {
        String queryPattern = "DROP\\s+TABLE\\s+(\\S+)\\.(\\S+)";
        Matcher matcher = getMatcher(sql, queryPattern);
        if (matcher.find()) {
            query.setDataBase(matcher.group(DATABASE_INDEX));
            query.setTable(matcher.group(TABLE_INDEX));
        }
    }

    void parseDelete(String sql, Query query) {
        String queryPattern = "DELETE\\s+FROM\\s+(\\S+)\\.(\\S+)";
        Matcher matcher = getMatcher(sql, queryPattern);
        if (matcher.find()) {
            query.setDataBase(matcher.group(DATABASE_INDEX));
            query.setTable(matcher.group(TABLE_INDEX));
        }
    }

    void parseUpdate(String sql, Query query) {
        String queryPattern = "UPDATE\\s+(\\S+)\\.(\\S+)\\s+SET\\s+(\\S+)\\s*=\\s*(\\S+)";
        Matcher matcher = getMatcher(sql, queryPattern);
        if (matcher.find()) {
            query.setDataBase(matcher.group(DATABASE_INDEX));
            query.setTable(matcher.group(TABLE_INDEX));
            HashMap<String, String> data = new HashMap<>();
            data.put(matcher.group(COLUMN_INDEX), matcher.group(DATA_INDEX));
            query.setData(data);
        }
    }

    void parseCreateTable(String sql, Query query) {
        String queryPattern = "CREATE\\s+TABLE\\s+(\\S+)\\.(\\S+)\\s*\\((.+)\\)";
        Matcher matcher = getMatcher(sql, queryPattern);
        if (matcher.find()) {
            query.setDataBase(matcher.group(DATABASE_INDEX));
            query.setTable(matcher.group(TABLE_INDEX));
            String metadata = matcher.group(METADATA_INDEX);
            query.setMetadata(Arrays.asList(metadata.split(",\\s")));
        }
    }

    void parseCreateDatabase(String sql, Query query) {
        String queryPattern = "CREATE\\s+DATABASE\\s+(\\S+)";
        Matcher matcher = getMatcher(sql, queryPattern);
        if (matcher.find()) {
            query.setDataBase(matcher.group(DATABASE_INDEX));
        }
    }

    void parseSelect(String sql, Query query) {
        String queryPattern = "SELECT\\s+\\*\\s+FROM\\s+(\\S+)\\.(\\S+)";
        Matcher matcher = getMatcher(sql, queryPattern);
        if (matcher.find()) {
            query.setDataBase(matcher.group(DATABASE_INDEX));
            query.setTable(matcher.group(TABLE_INDEX));
        }
    }

    private Matcher getMatcher(String sql, String queryPattern) {
        Pattern pattern = Pattern.compile(queryPattern, Pattern.CASE_INSENSITIVE);
        return pattern.matcher(sql);
    }

}
