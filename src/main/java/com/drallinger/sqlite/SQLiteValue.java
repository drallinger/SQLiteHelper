package com.drallinger.sqlite;

public class SQLiteValue <T> {
    public enum ValueType{
        INTEGER,
        REAL,
        TEXT
    }
    private final T value;
    private final ValueType type;

    private SQLiteValue(T value, ValueType type){
        this.value = value;
        this.type = type;
    }

    public static SQLiteValue<Integer> integer(int value){
        return new SQLiteValue<>(value, ValueType.INTEGER);
    }

    public static SQLiteValue<Double> real(double value){
        return new SQLiteValue<>(value, ValueType.REAL);
    }

    public static SQLiteValue<String> text(String value){
        return new SQLiteValue<>(value, ValueType.TEXT);
    }

    public static SQLiteValue<Integer> bool(boolean value){
        return new SQLiteValue<>(value ? 1 : 0, ValueType.INTEGER);
    }

    public static boolean convertToBoolean(int i){
        return switch(i){
            case 1, 0 -> i == 1;
            default -> throw new IllegalArgumentException(
                "The given integer must be either 1 or 0 to be converted to a boolean"
            );
        };
    }

    public T getValue(){
        return value;
    }

    public ValueType getType(){
        return type;
    }
}
