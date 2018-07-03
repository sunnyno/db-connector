package com.dzytsuik.dbconnector.parser.stax

import org.junit.Test

import static org.junit.Assert.assertNotNull

class StaxQueryParserTest {
    @Test
    void getRowTest() {
        def stream = getClass().getResourceAsStream("/selectResult.xml") as InputStream
        StaxQueryParser staxQueryParser = new StaxQueryParser(stream)
        def table = "user"
        def dataBase = "db"
        def row = staxQueryParser.getRow(table, dataBase) as Map<String, String>
        def expected = new HashMap<String, String>()
        expected.put("id", "1")
        expected.put("name", "John")
        expected.put("age", "25")
        row.each { assertNotNull(expected.remove(it.key)) }

        def row2 = staxQueryParser.getRow(table, dataBase) as Map<String, String>
        expected.put("id", "2")
        expected.put("name", "Jessica")
        expected.put("age", "15")
        row2.each { assertNotNull(expected.remove(it.key)) }

        def row3 = staxQueryParser.getRow(table, dataBase) as Map<String, String>
        expected.put("id", "3")
        expected.put("name", "Zhenya")
        expected.put("age", "23")
        row3.each { assertNotNull(expected.remove(it.key)) }

    }
}
