package com.autumncode.contains;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import java.util.StringJoiner;

public class Runner {
    @Parameter
    String source = "h2";

    public static void main(String[] args) {
        Runner runner = new Runner();
        new JCommander(runner);
        runner.run();
    }

    private void run() {
        Properties properties = loadProperties(source);
        prepareDatabase(properties);
        showIn(properties);
    }

    private void showIn(Properties properties) {
        try (Connection conn = getConnection(properties)) {
            Integer[] data = {3, 4, 6, 11};
            if ("true".equals(properties.getProperty("supported"))) {
                String query = properties.getProperty("query");
                try (PreparedStatement ps = conn.prepareStatement(query)) {
                    ps.setObject(1, data);
                    try (ResultSet rs = ps.executeQuery()) {
                        showResults(rs);

                    }
                }
            } else {
                StringJoiner joiner = new StringJoiner(",", "select * from information where info in (", ")");
                for (Object ignored : data) {
                    joiner.add("?");
                }
                String query = joiner.toString();
                try (PreparedStatement ps = conn.prepareStatement(query)) {
                    for (int c = 0; c < data.length; c++) {
                        ps.setObject(c + 1, data[c]);
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        showResults(rs);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void showResults(ResultSet rs) throws SQLException {
        ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();
        for (int c = 1; c < columnCount + 1; c++) {
            System.out.printf("%10s ", metadata.getColumnLabel(c));
        }
        System.out.println();
        while (rs.next()) {
            for (int c = 1; c < columnCount + 1; c++) {
                System.out.printf("%10s ", rs.getObject(c));
            }
            System.out.println();
        }
    }

    private void prepareDatabase(Properties properties) {
        try (Connection conn = getConnection(properties)) {
            try (PreparedStatement ps = conn.prepareStatement(properties.getProperty("ddl"))) {
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(properties.getProperty("clear"))) {
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(properties.getProperty("insert"))) {
                for (int i = 0; i < 20; i++) {
                    ps.setInt(1, i);
                    ps.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection(Properties properties) throws SQLException {
        return DriverManager.getConnection(
                properties.getProperty("url"),
                properties.getProperty("user"),
                properties.getProperty("password"));
    }

    Properties loadProperties(String prefix) {
        String propertySource = "/" + prefix + ".properties";
        Properties properties = new Properties();
        try (InputStream propSource = this.getClass().getResourceAsStream(propertySource)) {
            properties.load(propSource);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }
}
