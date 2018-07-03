package com.dzytsuik.dbconnector.driver

import com.dzytsuik.dbconnector.entity.Query
import org.junit.Test

import static org.junit.Assert.*

class CustomResultSetTest {
    @Test
    void nextTest() {
        def stream = getClass().getResourceAsStream("/selectResult.xml") as BufferedInputStream
        def query = [getTable: "user", getDataBase: "db"] as Query
        CustomResultSet customResultSet = new CustomResultSet(stream, query)
        assertTrue(customResultSet.next())
        assertTrue(customResultSet.next())
        assertTrue(customResultSet.next())
        assertFalse(customResultSet.next())
    }

    @Test
    void nextEmptyTest() {
        def stream = getClass().getResourceAsStream("/emptyResult.xml") as BufferedInputStream
        def query = [getTable: "user", getDataBase: "db"] as Query
        CustomResultSet customResultSet = new CustomResultSet(stream, query)
        assertFalse(customResultSet.next())
    }


    @Test
    void getStringTest() {
        def stream = getClass().getResourceAsStream("/selectResult.xml") as BufferedInputStream
        def query = [getTable: "user", getDataBase: "db"] as Query
        CustomResultSet customResultSet = new CustomResultSet(stream, query)
        customResultSet.next()
        assertEquals("1", customResultSet.getString("id"))
        customResultSet.next()
        assertEquals("Jessica", customResultSet.getString("name"))
        customResultSet.next()
        assertEquals("23", customResultSet.getString("age"))

    }

    @Test
    void getMetaDataTest() {
        def stream = getClass().getResourceAsStream("/selectResult.xml") as BufferedInputStream
        def query = [getTable: "user", getDataBase: "db"] as Query
        CustomResultSet customResultSet = new CustomResultSet(stream, query)
        def expected = ["id", "name", "age"]
        def data = customResultSet.getMetaData() as CustomResultSetMetaData
        assertEquals(expected, data.metadata)
        //next still ok
        customResultSet.next()
        assertEquals("1", customResultSet.getString("id"))
    }

    @Test
    void getMetaDataEmptyTest() {
        def stream = getClass().getResourceAsStream("/emptyResult.xml") as BufferedInputStream
        def query = [getTable: "user", getDataBase: "db"] as Query
        CustomResultSet customResultSet = new CustomResultSet(stream, query)
        def data = customResultSet.getMetaData() as CustomResultSetMetaData
        assertTrue(data.metadata.isEmpty())
        //next still ok
        assertFalse(customResultSet.next())
    }
}
