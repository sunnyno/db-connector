package com.dzytsuik.dbconnector.parser.stax;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StaxQueryParser {
    private static final byte ESCAPE_CHAR = '\\';
    private static final XMLInputFactory XML_INPUT_FACTORY = XMLInputFactory.newInstance();
    private XMLEventReader reader;
    private InputStream inputStream;

    public StaxQueryParser(InputStream inputStream) throws SQLException {
        try {
            this.inputStream = inputStream;
            inputStream.mark(0);
            if ( inputStream.read() != ESCAPE_CHAR) {
                inputStream.reset();
                inputStream.mark(0);
                reader = XML_INPUT_FACTORY.createXMLEventReader(inputStream);
            }
        } catch (XMLStreamException | IOException e) {
            throw new SQLException("Error getting result set", e);
        }
    }

    public List<String> getMetadata(String table) throws XMLStreamException, IOException {
        List<String> metadata = new ArrayList<>();

        boolean isStartRow = false;
        while (reader != null && reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            int eventType = xmlEvent.getEventType();
            if (xmlEvent.toString().contains(table) && eventType == XMLStreamConstants.START_ELEMENT) {
                isStartRow = true;
                continue;
            }
            if (isStartRow) {
                if (eventType == XMLStreamConstants.START_ELEMENT) {
                    metadata.add(xmlEvent.asStartElement().getName().getLocalPart());
                }
            }
            if (xmlEvent.toString().contains(table) && eventType == XMLStreamConstants.END_ELEMENT) {
                break;
            }
        }
        if (reader != null) {
            inputStream.reset();
            reader = XML_INPUT_FACTORY.createXMLEventReader(inputStream);
        }
        return metadata;
    }

    public Map<String, String> getRow(String table, String database) throws XMLStreamException {
        Map<String, String> dataMap = new LinkedHashMap<>();
        boolean isStartRow = false;
        boolean isColumn = false;
        String data;
        String metadata = "";
        while (reader != null && reader.hasNext()) {
            XMLEvent xmlEvent = reader.nextEvent();
            int eventType = xmlEvent.getEventType();
            if (xmlEvent.toString().contains(table) && eventType == XMLStreamConstants.START_ELEMENT) {
                isStartRow = true;
                continue;
            }
            if (isStartRow) {
                if (eventType == XMLStreamConstants.START_ELEMENT) {
                    metadata = xmlEvent.asStartElement().getName().getLocalPart();
                    isColumn = true;
                }
                if (eventType == XMLStreamConstants.CHARACTERS && isColumn) {
                    data = xmlEvent.asCharacters().getData();
                    dataMap.put(metadata, data);
                    isColumn = false;
                }
            }
            if ((xmlEvent.toString().contains(table) || xmlEvent.toString().contains(database))
                    && eventType == XMLStreamConstants.END_ELEMENT) {
                break;
            }
        }
        return dataMap;
    }
}
