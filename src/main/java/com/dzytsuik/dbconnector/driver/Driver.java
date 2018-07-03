package com.dzytsuik.dbconnector.driver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;


public class Driver implements java.sql.Driver {

    private static final String HOST = "host";
    private static final String PORT = "port";

    public Driver() {
    }

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException var1) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    public Connection connect(String url, Properties info) throws SQLException {
        try {
            String host = info.getProperty(HOST);
            int port = Integer.parseInt(info.getProperty(PORT));
            return new CustomConnection(host, port);
        } catch (IOException e) {
            throw new SQLException("Error establishing connection");
        }
    }

    public boolean acceptsURL(String url) {
        return false;
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return null;
    }

    public int getMajorVersion() {
        return 1;
    }

    public int getMinorVersion() {
        return 0;
    }

    public boolean jdbcCompliant() {
        return true;
    }

    public Logger getParentLogger() {
        return Logger.getLogger("com.dzytsuik.dbconnector");
    }
}
