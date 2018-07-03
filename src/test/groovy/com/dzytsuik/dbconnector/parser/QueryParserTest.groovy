package com.dzytsuik.dbconnector.parser

import com.dzytsuik.dbconnector.entity.Query
import com.dzytsuik.dbconnector.entity.QueryType
import org.junit.Test

import static org.junit.Assert.*

class QueryParserTest {

    @Test
    void parseQueryTest() {
        def sql = "select * from test.user"
        String parsed = "{\"database\":\"test\",\"type\":\"select\",\"table\":\"user\"}"
        Query query = new Query()
        QueryParser queryParser = new QueryParser(query: query)
        def actual = queryParser.parseQuery(sql)
        assertEquals(parsed, actual)
    }

    @Test
    void getQueryTest() {
        def sql = "drop database test"
        QueryParser queryParser = new QueryParser()
        Query actualQuery = queryParser.getQuery(sql)
        Query expectedQuery = new Query(dataBase: "test", type: QueryType.DROP_DATABASE)
        assertEquals(expectedQuery, actualQuery)
    }

    @Test
    void parseDropDatabase() {
        def sql = "drop database test"
        QueryParser queryParser = new QueryParser()
        Query actualQuery = new Query()
        Query expectedQuery = new Query(dataBase: "test")
        queryParser.parseDropDatabase(sql, actualQuery)
        assertEquals(expectedQuery, actualQuery)
    }

    @Test
    void parseDropTableTest() {
        def sql = "drop table test.user"
        QueryParser queryParser = new QueryParser()
        Query actualQuery = new Query()
        Query expectedQuery = new Query(dataBase: "test", table: "user")
        queryParser.parseDropTable(sql, actualQuery)
        assertEquals(expectedQuery, actualQuery)
    }

    @Test
    void parseDeleteTest() {
        def sql = "delete from test.user"
        QueryParser queryParser = new QueryParser()
        Query actualQuery = new Query()
        Query expectedQuery = new Query(dataBase: "test", table: "user")
        queryParser.parseDelete(sql, actualQuery)
        assertEquals(expectedQuery, actualQuery)
    }

    @Test
    void parseUpdateTest() {
        def sql = "update test.user set id=1"
        QueryParser queryParser = new QueryParser()
        Query actualQuery = new Query()
        Query expectedQuery = new Query(dataBase: "test", table: "user", data: ["id": "1"])
        queryParser.parseUpdate(sql, actualQuery)
        assertEquals(expectedQuery, actualQuery)
    }

    @Test
    void parseInsertTest() {
        def sql = "insert into test.user(id, name) values(1, John)"
        QueryParser queryParser = new QueryParser()
        Query actualQuery = new Query()
        Query expectedQuery = new Query(dataBase: "test", table: "user", metadata: ["id", "name"], data: ["id": "1", name: "John"])
        queryParser.parseInsert(sql, actualQuery)
        assertEquals(expectedQuery, actualQuery)
    }

    @Test
    void parseCreateTableTest() {
        def sql = "create table test.user(id, name)"
        QueryParser queryParser = new QueryParser()
        Query actualQuery = new Query()
        Query expectedQuery = new Query(dataBase: "test", table: "user", metadata: ["id", "name"])
        queryParser.parseCreateTable(sql, actualQuery)
        assertEquals(expectedQuery, actualQuery)
    }

    @Test
    void parseCreateDatabaseTest() {
        def sql = "create database test"
        QueryParser queryParser = new QueryParser()
        Query actualQuery = new Query()
        Query expectedQuery = new Query(dataBase: "test")
        queryParser.parseCreateDatabase(sql, actualQuery)
        assertEquals(expectedQuery, actualQuery)
    }

    @Test
    void parseSelectTest() {
        def sql = "select * from test.user"
        QueryParser queryParser = new QueryParser()
        Query actualQuery = new Query()
        Query expectedQuery = new Query(dataBase: "test", table: "user")
        queryParser.parseSelect(sql, actualQuery)
        assertEquals(expectedQuery, actualQuery)
    }

}
