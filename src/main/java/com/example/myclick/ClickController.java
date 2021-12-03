package com.example.myclick;


import com.example.myclick.utilities.ClickHouseUtility;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

@RestController
public class ClickController {

    @GetMapping("/query")
    public List<Map<String, Object>> query(@RequestParam(value = "sql", defaultValue = "") String sql) {
        return ClickHouseUtility.sqlQuery(sql);
    }

    @GetMapping("/addquery")
    public String query() throws SQLException, ParseException {
        ClickHouseUtility.insertData();
        return "OK";
    }
}