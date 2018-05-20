package com.tinkerpop.blueprints.impls.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.topicquests.blueprints.pg.BlueprintsEdgeIterable;
import org.topicquests.blueprints.pg.BlueprintsVertexIterable;
import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Contains;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * @author Lukas Krejci
 * @since 1.0
 */
public final class SqlVertexQuery implements VertexQuery {
	private PostgresConnectionFactory provider;

    private final SqlGraph graph;
    private final String rootVertexId;
    private final QueryFilters filters = new QueryFilters();
    private int limit = -1;
    private Direction direction = Direction.OUT;

    public SqlVertexQuery(SqlGraph graph, String rootVertexId) {
    	System.out.println("SqlVertexQuery- "+rootVertexId);
        this.graph = graph;
        provider = graph.getProvider();
        this.rootVertexId = rootVertexId;
    }

    @Override
    public SqlVertexQuery direction(Direction direction) {
        this.direction = direction;
        return this;
    }

    @Override
    public SqlVertexQuery labels(String... labels) {
        has("label", Contains.IN, Arrays.asList(labels));
        return this;
    }

    @Override
    public long count() {
        try {
        		
        	IResult r = generateCountEdgeQuery();
        	ResultSet rs = (ResultSet)r.getResultObject();
        	if (rs != null) {
                if (!rs.next()) {
                    return 0;
                }

                long cnt = rs.getLong(1);

                if (limit < 0) {
                    return cnt;
                } else {
                    return cnt > limit ? limit : cnt;
                }
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
        return -1;
    }

    @Override
    public Object vertexIds() {
        return null; // TODO what's this?
    }

    @Override
    public SqlVertexQuery has(String key) {
        filters.has(key);
        return this;
    }

    @Override
    public SqlVertexQuery hasNot(String key) {
        filters.hasNot(key);
        return this;
    }

    @Override
    public SqlVertexQuery has(String key, Object value) {
        filters.has(key, value);
        return this;
    }

    @Override
    public SqlVertexQuery hasNot(String key, Object value) {
        filters.hasNot(key, value);
        return this;
    }

    @Override
    public SqlVertexQuery has(String key, Predicate predicate, Object value) {
        filters.has(key, predicate, value);
        return this;
    }

    @Override
    public <T extends Comparable<T>> VertexQuery has(String key, T value, Compare compare) {
        filters.has(key, value, compare);
        return this;
    }

    @Override
    public <T extends Comparable<?>> VertexQuery interval(String key, T startValue, T endValue) {
        filters.interval(key, startValue, endValue);
        return this;
    }

    @Override
    public VertexQuery limit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public CloseableIterable<Edge> edges() {
        try {
            IResult r = generateEdgeQuery();
            ResultSet rs = (ResultSet)r.getResultObject();
            return (CloseableIterable<Edge>)new BlueprintsEdgeIterable(graph, rs);
         } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    
    @Override
    public CloseableIterable<Vertex> vertices() {
        try {
            IResult r = generateVertexQuery();
            //long artificialLimit = direction == Direction.BOTH ? limit : -1;
            //return new ResultSetIterable<Vertex>(SqlVertex.GENERATOR, graph, stmt.executeQuery(), artificialLimit);
            ResultSet rs = (ResultSet)r.getResultObject();
            return (CloseableIterable<Vertex>)new BlueprintsVertexIterable(graph, rs);
       } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    private IResult generateCountEdgeQuery() throws SQLException {
        return generateQuery("SELECT COUNT(*)");
    }

    private IResult generateEdgeQuery() throws SQLException {
        return generateQuery("SELECT id, vertex_in, vertex_out, label");
    }

    /**
     * The nature of the database is that the vertex does NOT know its
     * in and out edges. We have two options <br/>
     * a) modify the schema to enable a vertex to know its in and out edges<br/>
     * b) modify this query mechanism to search edges for both in and out vertex identities<br/>
     * 
     * @return
     * @throws SQLException
     */
    private IResult generateVertexQuery() throws SQLException {
        String select;
        switch (direction) {
        case IN:
            select = "SELECT vertex_out";
            return generateQuery(select);
        case OUT:
            select = "SELECT vertex_in";
            return generateQuery(select);
        case BOTH:
            direction = Direction.IN;
            QueryFilters.SqlAndParams sql = generateQueryString("SELECT vertex_out");
            direction = Direction.OUT;
            QueryFilters.SqlAndParams sql2 = generateQueryString("SELECT vertex_in");
            direction = Direction.BOTH;

            sql.sql.append(" UNION ALL ").append(sql2.sql);
            sql.params.addAll(sql2.params);

            return generateQuery(sql);
        default:
            throw new IllegalStateException("unknown direction value");
        }

    }

    private IResult generateQuery(String select) throws SQLException {
        return generateQuery(generateQueryString(select));
    }

    private IResult generateQuery(QueryFilters.SqlAndParams sql) throws SQLException {
  	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        conn = provider.getConnection();
        conn.setProxyRole(r);
        int len = sql.params.size();
        int i = 0;
        Object [] vals = new Object[len];
        for (Object p : sql.params) {
            vals[i++] = p;
        }
        conn.executeSelect(sql.sql.toString(), r, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, vals);

        return r;
    }

    private QueryFilters.SqlAndParams generateQueryString(String select) throws SQLException {
        String edges = graph.getEdgesTableName();

        String directionFilter = null;
        switch (direction) {
        case IN:
            directionFilter = edges + ".vertex_in = ?";
            break;
        case OUT:
            directionFilter = edges + ".vertex_out = ?";
            break;
        case BOTH:
            directionFilter = "(" + edges + ".vertex_in = ? OR " + edges + ".vertex_out = ?)";
            break;
        }

        QueryFilters.SqlAndParams sql = filters.generateStatement(select, graph.getEdgesTableName(),
            graph.getEdgePropertiesTableName(),
            SqlEdge.getPropertyTableForeignKey(), SqlEdge.DISALLOWED_PROPERTY_NAMES, directionFilter);


        if (limit >= 0) {
            sql.sql.append(" LIMIT ").append(limit);
        }

        sql.params.add(0, rootVertexId);
        if (direction == Direction.BOTH) {
            sql.params.add(0, rootVertexId);
        }

        return sql;
    }
}
