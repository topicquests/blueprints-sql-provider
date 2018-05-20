package com.tinkerpop.blueprints.impls.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 * @author Lukas Krejci
 * @since 1.0
 * @author for changes: park
 */
public class SqlEdge extends SqlElement implements Edge {

    public static final List<String> DISALLOWED_PROPERTY_NAMES = Arrays.asList("id", "label");

    private final String inVertexId;
    private final String outVertexId;
    private SqlVertex inVertex;
    private SqlVertex outVertex;

    public SqlEdge(SqlGraph graph, String id, String inVertexId, String outVertexId, String label) {
        super(graph, id, label);
        this.inVertexId = inVertexId;
        this.outVertexId = outVertexId;
    }

    public static String getPropertyTableForeignKey() {
        return "edge_id";
    }

    @Override
    public void remove() {
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);

        	conn.beginTransaction(r);
            String sql = "DELETE FROM tq_graph.edges WHERE id = ?";
            conn.executeSQL(sql, r, getId());
            conn.endTransaction(r);
            conn.closeConnection(r);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    protected String getPropertiesTableName() {
        return graph.getEdgePropertiesTableName();
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
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        SqlVertex v = null;
        switch (direction) {
        case BOTH:
            throw new IllegalArgumentException();
        case IN:
            if (inVertex == null) {
                inVertex = graph.getVertex(Long.valueOf(inVertexId));
            }
            v = inVertex;
            break;
        case OUT:
            if (outVertex == null) {
                outVertex = graph.getVertex(outVertexId);
            }
            v = outVertex;
            break;
        }

        return v;
    }

 //   @Override

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        SqlEdge sqlEdge = (SqlEdge) o;

        if (inVertexId != sqlEdge.inVertexId) {
            return false;
        }
        if (outVertexId != sqlEdge.outVertexId) {
            return false;
        }
        if (!label.equals(sqlEdge.label)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (inVertexId.hashCode());
        result = 31 * result + (int) (outVertexId.hashCode());
        result = 31 * result + label.hashCode();
        return result;
    }

	@Override
	protected String getTableName() {
		return "edges";
	}
}
