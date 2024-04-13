package com.drallinger.sqlite;

public class SQLiteFunctions {
    public static SQLiteFunction<Integer> singleInteger(){
        return rs -> rs.getInt(1);
    }

    public static SQLiteFunction<Double> singleDouble(){
        return rs -> rs.getDouble(1);
    }

    public static SQLiteFunction<String> singleString(){
        return rs -> rs.getString(1);
    }
}
