package com.drallinger.sqlite.blueprints;

public record PreparedStatementBlueprint(String query, boolean returnKeys) {}
