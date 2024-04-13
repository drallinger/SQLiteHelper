package com.drallinger.sqlite;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface SQLiteFunction<T> {
    T execute(ResultSet rs) throws SQLException;
}
