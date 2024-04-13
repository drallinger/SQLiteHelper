package com.drallinger.sqlite;

import com.drallinger.sqlite.blueprints.PreparedStatementBlueprint;
import com.drallinger.sqlite.blueprints.TableBlueprint;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class SQLiteHelper {
    private static final String BASE_CONNECTION_URL = "jdbc:sqlite:%s";
    private static final String DEFAULT_DATABASE_FILE = ":memory:";
    private final String connectionUrl;
    private final HashMap<String, PreparedStatementBlueprint> preparedStatementBlueprints;
    private final HashMap<String, SQLiteFunction<?>> functions;

    private SQLiteHelper(Builder builder){
        connectionUrl = String.format(BASE_CONNECTION_URL, builder.databaseFile);
        preparedStatementBlueprints = builder.preparedStatementBlueprints;
        functions = builder.functions;
        if(!builder.tables.isEmpty()){
            createTables(builder.tables);
        }
    }

    public static class Builder{
        private final String databaseFile;
        private final ArrayList<TableBlueprint> tables;
        private final HashMap<String, PreparedStatementBlueprint> preparedStatementBlueprints;
        private final HashMap<String, SQLiteFunction<?>> functions;

        private Builder(String databaseFile){
            this.databaseFile = databaseFile;
            tables = new ArrayList<>();
            preparedStatementBlueprints = new HashMap<>();
            functions = new HashMap<>();
        }

        public Builder table(String tableName, boolean ifNotExists, String... columns){
            tables.add(new TableBlueprint(tableName, ifNotExists, columns));
            return this;
        }

        public Builder table(String tableName, String... columns){
            return table(tableName, true, columns);
        }

        public Builder preparedStatement(String queryName, String query, boolean returnKeys){
            preparedStatementBlueprints.put(queryName, new PreparedStatementBlueprint(query, returnKeys));
            return this;
        }

        public Builder prepareStatement(String queryName, String query){
            return preparedStatement(queryName, query, false);
        }

        public Builder function(String functionName, SQLiteFunction<?> function){
            functions.put(functionName, function);
            return this;
        }

        public SQLiteHelper build(){
            return new SQLiteHelper(this);
        }
    }

    public static Builder newBuilder(String databaseFile){
        return new Builder(databaseFile);
    }

    public static Builder newBuilder(){
        return newBuilder(DEFAULT_DATABASE_FILE);
    }

    public SQLiteConnection connect(String... prepareStatementNames){
        SQLiteConnection sqLiteConnection = null;
        try{
            Connection connection = DriverManager.getConnection(connectionUrl);
            HashMap<String, PreparedStatement> preparedStatements = createPreparedStatements(connection, prepareStatementNames);
            sqLiteConnection = new SQLiteConnection(connection, preparedStatements, functions);
        }catch (SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
        return sqLiteConnection;
    }

    public SQLiteConnection connect(){
        return connect(getAllPreparedStatementNames());
    }

    private void createTables(ArrayList<TableBlueprint> tables){
        try(Connection connection = DriverManager.getConnection(connectionUrl); Statement statement = connection.createStatement()){
            for(TableBlueprint tableBlueprint : tables){
                statement.executeUpdate(createTableQuery(tableBlueprint));
            }
        }catch(SQLException e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    private String createTableQuery(TableBlueprint tableBlueprint){
        StringBuilder query = new StringBuilder("create table ");
        if(tableBlueprint.ifNotExists()){
            query.append("if not exists ");
        }
        query.append(tableBlueprint.tableName()).append("(");
        for(String column : tableBlueprint.columns()){
            query.append(column).append(",");
        }
        query.replace(query.length() - 1, query.length(), ");");
        return query.toString();
    }

    private String[] getAllPreparedStatementNames(){
        Set<String> keySet = preparedStatementBlueprints.keySet();
        return keySet.toArray(new String[0]);
    }

    private HashMap<String, PreparedStatement> createPreparedStatements(Connection connection, String[] prepareStatementNames) throws SQLException{
        HashMap<String, PreparedStatement> preparedStatements = new HashMap<>();
        for(String preparedStatementName : prepareStatementNames){
            if(!preparedStatementBlueprints.containsKey(preparedStatementName)){
                System.err.printf("No prepare statement configured with the name \"%s\"%n", preparedStatementName);
                System.exit(1);
            }
            PreparedStatementBlueprint preparedStatementBlueprint = preparedStatementBlueprints.get(preparedStatementName);
            preparedStatements.put(
                preparedStatementName,
                connection.prepareStatement(
                    preparedStatementBlueprint.query(),
                    preparedStatementBlueprint.returnKeys() ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS
                )
            );
        }
        return preparedStatements;
    }
}
