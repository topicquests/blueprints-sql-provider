package com.tinkerpop.blueprints.impls.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Lukas Krejci
 * @since 1.0
 * @author for adaptations: park
 */
final class Statements {

    private final SqlGraph graph;

    public Statements(SqlGraph graph) {
        this.graph = graph;
    }

    public PreparedStatement getAddVertex(Connection conn) throws SQLException {
        String sql = "INSERT INTO vertices (id, label) VALUES (?, ?)";
        return conn.prepareStatement(sql,
            Statement.RETURN_GENERATED_KEYS);
    }

    public PreparedStatement getAddEdge(Connection conn, String id, String inVertexId, String outVertexId, String label) throws SQLException {
        String sql = "INSERT INTO edges (id, vertex_in, vertex_out, label) VALUES (?, ?, ?, ?)";
        PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        stmt.setString(1, id);
        stmt.setString(2, inVertexId);
        stmt.setString(3, outVertexId);
        stmt.setString(4, label);
        return stmt;
    }

    public PreparedStatement getGetEdge(Connection conn, String id) throws SQLException {
        String sql = "SELECT id, vertex_in, vertex_out, label FROM edges WHERE id = ?";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, id);
        return stmt;
    }

    public PreparedStatement getGetVertex(Connection conn, String id) throws SQLException {
        String sql = "SELECT id, label FROM vertices WHERE id = ?";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, id);
        return stmt;
    }

    public PreparedStatement getRemoveVertex(Connection conn, String id) throws SQLException {
        String sql = "DELETE FROM vertices WHERE id = ?";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, id);
        return stmt;
    }

    public PreparedStatement getRemoveEdge(Connection conn, String id) throws SQLException {
        String sql = "DELETE FROM edges WHERE id = ?";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, id);
        return stmt;
    }

    public PreparedStatement getAllVertices(Connection conn) throws SQLException {
        String sql = "SELECT id FROM vertices";
        return conn
            .prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    public PreparedStatement getAllEdges(Connection conn) throws SQLException {
        String sql = "SELECT id, vertex_in, vertex_out, label FROM edges";
        return conn
            .prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    public SqlVertex fromVertexResultSet(ResultSet rs) throws SQLException {
        if (!rs.next()) {
            return null;
        }

        return SqlVertex.GENERATOR.generate(graph, rs);
    }
}
