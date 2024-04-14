package com.drallinger.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;

public class SQLiteConnection implements AutoCloseable{
    private final Connection connection;
    private final HashMap<String, PreparedStatement> preparedStatements;
    private final HashMap<String, SQLiteFunction<?>> functions;

    public SQLiteConnection(Connection connection, HashMap<String, PreparedStatement> preparedStatements, HashMap<String, SQLiteFunction<?>> functions){
        this.connection = connection;
        this.preparedStatements = preparedStatements;
        this.functions = functions;
    }

    public Optional<String> execute(String queryName, boolean returnKeys, SQLiteValue<?>... values){
        Optional<String> optional = Optional.empty();
        try{
            PreparedStatement statement = preparedStatements.get(queryName);
            setStatementValues(statement, values);
            statement.executeUpdate();
            if(returnKeys){
                try(ResultSet resultSet = statement.getGeneratedKeys()){
                    if(resultSet.next()){
                        optional = Optional.of(resultSet.getString(1));
                    }
                }
            }
        }catch(SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
        return optional;
    }

    public Optional<String> execute(String queryName, SQLiteValue<?>... values){
        return execute(queryName, false, values);
    }

    public <T> T queryValue(String queryName, SQLiteFunction<T> function, SQLiteValue<?>... values){
        T result = null;
        try{
            PreparedStatement statement = preparedStatements.get(queryName);
            setStatementValues(statement, values);
            try(ResultSet resultSet = statement.executeQuery()){
                if(resultSet.next()){
                    result = function.execute(resultSet);
                }
            }
        }catch (SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
        return result;
    }

    public <T> T queryValue(String queryName, String functionName, SQLiteValue<?>... values){
        SQLiteFunction<T> function = (SQLiteFunction<T>) functions.get(functionName);
        return queryValue(queryName, function, values);
    }

    public <T> ArrayList<T> queryValues(String queryName, SQLiteFunction<T> function, SQLiteValue<?>... values){
        ArrayList<T> arrayList = new ArrayList<>();
        try{
            PreparedStatement statement = preparedStatements.get(queryName);
            setStatementValues(statement, values);
            try(ResultSet resultSet = statement.executeQuery()){
                while(resultSet.next()){
                    arrayList.add(function.execute(resultSet));
                }
            }
        }catch (SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
        return arrayList;
    }

    public <T> ArrayList<T> queryValues(String queryName, String functionName, SQLiteValue<?>... values){
        SQLiteFunction<T> function = (SQLiteFunction<T>) functions.get(functionName);
        return queryValues(queryName, function, values);
    }

    public boolean queryBoolean(String queryName, SQLiteValue<?>... values){
        boolean result = false;
        try{
            PreparedStatement statement = preparedStatements.get(queryName);
            setStatementValues(statement, values);
            try(ResultSet resultSet = statement.executeQuery()){
                if(resultSet.next()){
                    result = (resultSet.getInt(1) == 1);
                }
            }
        }catch (SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
        return result;
    }

    public void setAutoCommit(boolean autoCommit){
        try{
            connection.setAutoCommit(autoCommit);
        }catch(SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void commit(){
        try{
            connection.commit();
        }catch(SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void rollback(){
        try{
            connection.rollback();
        }catch(SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    public boolean isConnected(){
        boolean result = false;
        try{
            result = !connection.isClosed();
        }catch(SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        try{
            if(connection != null && !connection.isClosed()){
                connection.close();
            }
        }catch(SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void setStatementValues(PreparedStatement statement, SQLiteValue<?>[] values) throws SQLException{
        for(int i = 0; i < values.length; i++){
            SQLiteValue<?> value = values[i];
            switch (value.getType()){
                case INTEGER -> statement.setInt(i+1, (int) value.getValue());
                case REAL -> statement.setDouble(i+1, (double) value.getValue());
                case TEXT -> statement.setString(i+1, (String) value.getValue());
            }
        }
    }
}
