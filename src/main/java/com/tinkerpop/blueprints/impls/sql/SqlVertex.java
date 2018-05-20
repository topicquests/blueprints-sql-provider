package com.tinkerpop.blueprints.impls.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.topicquests.blueprints.pg.BlueprintsEdgeIterable;
import org.topicquests.blueprints.pg.BlueprintsVertexIterable;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 * @author Lukas Krejci
 * @since 1.0
 * @author for changes: park
 */
public class SqlVertex extends SqlElement implements Vertex {

    public static final List<String> DISALLOWED_PROPERTY_NAMES = Arrays.asList("id");

    public SqlVertex(SqlGraph graph, String id, String label) {
        super(graph, id, label);
    }

    public static String getPropertyTableForeignKey() {
        return "vertex_id";
    }

    @Override
    public void remove() {
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
        	conn.setProxyRole(r);
        	conn.beginTransaction(r);
            String sql = "DELETE FROM tq_graph.vertices WHERE id = ?";
            conn.executeSQL(sql, r, getId());
            conn.endTransaction(r);
            conn.closeConnection(r);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    protected String getPropertiesTableName() {
        return graph.getVertexPropertiesTableName();
    }

    @Override
    protected String getPropertyTableElementIdName() {
        return getPropertyTableForeignKey();
    }

    @Override
    protected List<String> getDisallowedPropertyNames() {
        return DISALLOWED_PROPERTY_NAMES;
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        StringBuilder sql = new StringBuilder(
            "SELECT e.id, e.vertex_in, e.vertex_out, e.label FROM edges e, vertices v WHERE v.id = ? ");

        switch (direction) {
        case IN:
            sql.append("AND e.vertex_in = v.id ");
            break;
        case OUT:
            sql.append("AND e.vertex_out = v.id ");
            break;
        case BOTH:
            sql.append("AND e.vertex_in = v.id ");
            addLabelConditions(sql, "e", labels);
            sql.append(
                " UNION ALL SELECT e.id, e.vertex_in, e.vertex_out, e.label FROM edges e, vertices v WHERE v.id = ? AND e.vertex_out = v.id ");
            break;
        }

        addLabelConditions(sql, "e", labels);

	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
        	conn.setProxyRole(r);
        	int len = (1+labels.length) * 2;
        	Object [] vals = new Object[len];
        	vals[0] = getId();
            int inc = 1;
            if (direction == Direction.BOTH) {
                for (int i = 0; i < labels.length; ++i) {
                    vals[i + inc] = labels[i];
                }

                inc += labels.length;

                vals[inc] = getId();

                inc++;
            }

            for (int i = 0; i < labels.length; ++i) {
                vals[i + inc] = labels[i];
            }
            conn.executeSelect(sql.toString(), r, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, vals);
        	
            ResultSet rs = (ResultSet)r.getResultObject();
            return (Iterable<Edge>)new BlueprintsEdgeIterable(graph, rs);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        StringBuilder sql = new StringBuilder("SELECT v.id, v.label FROM vertices v, edges e WHERE ");

        switch (direction) {
        case IN:
            sql.append("e.vertex_in = ? AND e.vertex_out = v.id ");
            break;
        case OUT:
            sql.append("e.vertex_out = ? AND e.vertex_in = v.id ");
            break;
        case BOTH:
            sql.append("e.vertex_in = ? AND e.vertex_out = v.id ");
            addLabelConditions(sql, "e", labels);
            sql.append(" UNION ALL SELECT v.id, v.label FROM vertices v, edges e WHERE e.vertex_out = ? AND e.vertex_in = v.id ");
            break;
        }

        addLabelConditions(sql, "e", labels);
System.out.println("SqlVertex.getVertices "+sql.toString());
		IPostgresConnection conn = null;
		IResult r = new ResultPojo();
		try {
			conn = provider.getConnection();
			conn.setProxyRole(r);
            int inc = 1;
            int len = (1+labels.length)*2;
            Object [] vals = new Object[len];
            vals[0] = getId();
            if (direction == Direction.BOTH) {
                for (int i = 0; i < labels.length; ++i) {
                	vals[i + inc] = labels[i];
                }

                inc += labels.length;

                vals[inc] = getId();

                inc++;
            }

            for (int i = 0; i < labels.length; ++i) {
            	vals[i + inc] = labels[i];
            }
            conn.executeSelect(sql.toString(), r, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, vals);
        	
            ResultSet rs = (ResultSet)r.getResultObject();
            return (Iterable<Vertex>)new BlueprintsVertexIterable(graph, rs);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public VertexQuery query() {
        return new SqlVertexQuery(graph, getId());
    }

    @Override
    public SqlEdge addEdge(String label, Vertex inVertex) {
        return graph.addEdge(null, this, inVertex, label);
    }

    private boolean addLabelConditions(StringBuilder sql, String tableName, String... labels) {
        if (labels == null)
        	return false;
    	if (labels.length > 0) {
            sql.append("AND ").append(tableName).append(".label IN (?");
        }

        for (int i = 1; i < labels.length; ++i) {
            sql.append(", ?");
        }

        if (labels.length > 0) {
            sql.append(") ");
        }

        return labels.length > 0;
    }

	@Override
	protected String getTableName() {
		return "vertices";
	}
}
