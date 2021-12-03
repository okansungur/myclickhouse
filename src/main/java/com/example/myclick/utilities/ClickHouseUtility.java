package com.example.myclick.utilities;


import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Date;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
@Component
public class ClickHouseUtility {

    private static Logger log = LoggerFactory.getLogger(ClickHouseUtility.class);

    private static String url;
    private static String username;
    private static String password;
    private static String db;
    private static Integer socketTimeout;

    @Value("${clickhouse.url}")
    public void setUrl(String url) {
        this.url = url;
    }

    @Value("${clickhouse.username}")
    public void setUsername(String username) {
        this.username = username;
    }

    @Value("${clickhouse.password}")
    public void setPassword(String password) {
        this.password = password;
    }

    @Value("${clickhouse.db}")
    public void setDb(String db) {
        this.db = db;
    }

    @Value("${clickhouse.socketTimeout}")
    public void setSocketTimeout(Integer socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public static Connection getConnection() {
        ClickHouseConnection conn = null;
        ClickHouseProperties properties = new ClickHouseProperties();
        log.info(connString());
        if (!StringUtils.isEmpty(username)) {
            properties.setUser(username);
        }
        if (!StringUtils.isEmpty(password)) {
            properties.setPassword(password);
        }
        if (!StringUtils.isEmpty(db)) {
            properties.setDatabase(db);
        }
        properties.setSocketTimeout(socketTimeout);

        try {
            ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
            conn = dataSource.getConnection();

        } catch (SQLException se) {
            log.error(connString() + " Exception: ", se);
        } catch (Exception e) {
            log.error(connString() + " Exception: ", e);
        } finally {
        }
        return conn;
    }

    public static void insertData() throws SQLException, ParseException {

        Connection connection = getConnection();
        if (connection != null) {


            connection.createStatement().execute("create DATABASE IF NOT EXISTS  tutorial");
            connection.createStatement().execute("DROP TABLE IF EXISTS tutorial.testet");
            connection.createStatement().execute(" truncate table  IF EXISTS tutorial.testet ");

            connection.createStatement().execute(
                    "CREATE TABLE tutorial.testet (" +
                            "date Date," +

                            "string String," +
                            "int32 Int32," +
                            "float64 Float64," +
                            "mymessage String" +
                            ") ENGINE = MergeTree(date, (date), 8192)"
            );


            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            dateFormat.setTimeZone(TimeZone.getDefault());
            Date date = new Date(dateFormat.parse("1977-09-05").getTime());

            String string = "User-Birthday";
            int int32 = (int) Math.ceil(Math.random() * 10000);
            double float64 = 43.44;
            String mymessage = "the logs are inserted";
            PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO tutorial.testet (date,  string, int32, float64,mymessage) VALUES (?, ?, ?, ?, ?)"
            );

            statement.setDate(1, date);

            statement.setString(2, string);
            statement.setInt(3, int32);
            statement.setDouble(4, float64);
            statement.setString(5, mymessage);
            statement.addBatch();
            statement.executeBatch();

        }

    }


    public static List<Map<String, Object>> sqlQuery(String sql) {
        log.info("Start " + sql);
        List<Map<String, Object>> data = new ArrayList<>();

        Connection conn = getConnection();
        if (conn == null) {
            Map<String, Object> error = new HashMap<>();
            error.put("Error: ", "Failed to get connection " + connString());
            data.add(error);
        } else {
            try {
                Statement statement = conn.createStatement();
                ResultSet results = statement.executeQuery(sql);
                ResultSetMetaData rsmd = results.getMetaData();
                while (results.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        row.put(rsmd.getColumnName(i), results.getString(i));
                    }
                    data.add(row);
                }
            } catch (SQLException e) {
                log.error(connString() + " Exception: ", e);
                Map<String, Object> error = new HashMap<>();
                error.put("Error: ", e);
                data.add(error);
            }
        }

        return data;
    }

    public static String connString() {
        return "Connect: url: " + url + " username: " + username + " password: " + password + " db: " + db + " socketTimeout: " + socketTimeout;
    }
}
