package com.tinkerpop.blueprints.impls.sql;

import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.WeakHashMap;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.ThreadedTransactionalGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;

import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;
import org.topicquests.pg.api.IPostgreSqlProvider;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 * @author Lukas Krejci
 * @since 1.0
 * @author for changes: park
 */
public final class SqlGraph implements ThreadedTransactionalGraph {
    private static final Features FEATURES = new Features();
    private BlueprintsPgEnvironment environment;
    static {
        FEATURES.supportsSerializableObjectProperty = false;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = false;
        FEATURES.supportsUniformListProperty = false;
        FEATURES.supportsMixedListProperty = false;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = false;
        FEATURES.supportsStringProperty = true;

        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.isPersistent = true;
        FEATURES.isWrapper = false;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsVertexIndex = false;
        FEATURES.supportsEdgeIndex = false;
        FEATURES.ignoresSuppliedIds = true;
        FEATURES.supportsTransactions = true;
        FEATURES.supportsIndices = false;
        FEATURES.supportsKeyIndices = false;
        FEATURES.supportsVertexKeyIndex = false;
        FEATURES.supportsEdgeKeyIndex = false;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;
        FEATURES.supportsThreadedTransactions = true;
        FEATURES.supportsThreadIsolatedTransactions = false;
    }

    private IPostgreSqlProvider provider;
   // private final DataSource dataSource;
   // private volatile Connection connection;
    private volatile Statements statements;
    private final String verticesTableName;
    private final String edgesTableName;
    private final String vertexPropertiesTableName;
    private final String edgePropertiesTableName;
    
	/**
	 * Pools Connections for each local thread
	 * Must be closed when the thread terminates
	 */
	private ThreadLocal<Connection> localMapConnection = new ThreadLocal<Connection>();

    //TODO should this be an LRUCache?
    private final WeakHashMap<String, WeakReference<SqlVertex>> vertexCache = new WeakHashMap<>();

   


    

    public SqlGraph(BlueprintsPgEnvironment env, IPostgreSqlProvider p) {
    	environment = env;
    	provider = p;
    	verticesTableName = "vertices";
        edgesTableName = "edges";
        vertexPropertiesTableName = "vertex_properties";
        edgePropertiesTableName = "edge_properties";
        statements = new Statements(this);
    }
    

    

    @Override
    public TransactionalGraph newTransaction() {
        return new SqlGraph(environment, provider);
    }

    @Override
    public void stopTransaction(Conclusion conclusion) {
        if (conclusion == Conclusion.SUCCESS) {
            commit();
        } else {
            rollback();
        }
    }

    @Override
    public void commit() {
        /*try {
            ensureConnection();
            connection.commit();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }*/
    }

    @Override
    public void rollback() {
       /* try {
            ensureConnection();
            connection.rollback();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }*/
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    @Override
    public Vertex addVertex(Object id) {
        return addVertex((String)id, "no label");
    }
    
    public Vertex addVertex(String id, String label) {
    	Connection conn = getConnection();
        try (PreparedStatement stmt = statements.getAddVertex(conn)) {
        	stmt.setString(1, id);
        	stmt.setString(2, label);
            if (stmt.executeUpdate() == 0) {
                return null;
            }
            //conn.commit();
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                return cache(statements.fromVertexResultSet(rs));
            }
            
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public SqlVertex getVertex(Object id) {
        String realId = getId(id);

        if (realId == null) {
            return null;
        }

        WeakReference<SqlVertex> ref = vertexCache.get(realId);

        SqlVertex v = ref == null ? null : ref.get();
        if (v != null) {
            return v;
        }

        Connection conn = getConnection();
        try (PreparedStatement stmt = statements.getGetVertex(conn, realId)) {
            if (!stmt.execute()) {
                return null;
            }

            try (ResultSet rs = stmt.getResultSet()) {
                return cache(statements.fromVertexResultSet(rs));
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public void removeVertex(Vertex vertex) {
    	Connection conn = getConnection();
        try (PreparedStatement stmt = statements.getRemoveVertex(conn, (String)vertex.getId())) {
            if (stmt.executeUpdate() == 0) {
                throw new IllegalStateException("Vertex with id " + vertex.getId() + " doesn't exist.");
            }
            vertexCache.remove(vertex.getId());
            //conn.commit();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public CloseableIterable<Vertex> getVertices() {
    	Connection conn = getConnection();
        try {
            PreparedStatement stmt = statements.getAllVertices(conn);

            if (!stmt.execute()) {
                stmt.close();
                return ResultSetIterable.empty();
            }

            return new ResultSetIterable<Vertex>(SqlVertex.GENERATOR, this, stmt.getResultSet());
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public CloseableIterable<Vertex> getVertices(String key, Object value) {
        return query().has(key, value).vertices();
    }

    @Override
    public SqlEdge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        if (label == null) {
            throw new IllegalArgumentException("null label");
        }

        Connection conn = getConnection();

        try (PreparedStatement stmt = statements
            .getAddEdge(conn, (String)id, (String) inVertex.getId(), (String) outVertex.getId(), label)) {

            if (stmt.executeUpdate() == 0) {
                return null;
            }
            //conn.commit();
            String eid = (String)id;
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (!rs.next()) {
                    return null;
                }

                eid = rs.getString("id");
            }

            try (ResultSet rs = statements.getGetEdge(conn, eid).executeQuery()) {
                if (!rs.next()) {
                    return null;
                }

                return SqlEdge.GENERATOR.generate(this, rs);
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public SqlEdge getEdge(Object id) {
        String eid = getId(id);
        if (eid == null) {
            return null;
        }

        Connection conn = getConnection();

        try (PreparedStatement stmt = statements.getGetEdge(conn, eid)) {
            if (!stmt.execute()) {
                return null;
            }

            try (ResultSet rs = stmt.getResultSet()) {
                if (!rs.next()) {
                    return null;
                }
                return SqlEdge.GENERATOR.generate(this, rs);
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public void removeEdge(Edge edge) {
    	Connection conn = getConnection();

        try (PreparedStatement stmt = statements.getRemoveEdge(conn, (String) edge.getId())) {
            if (stmt.executeUpdate() == 0) {
                throw new IllegalStateException("Edge with id " + edge.getId() + " doesn't exist.");
            }
            //conn.commit();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Iterable<Edge> getEdges() {
    	Connection conn = getConnection();

        try {
            PreparedStatement stmt = statements.getAllEdges(conn);
            return new ResultSetIterable<Edge>(SqlEdge.GENERATOR, this, stmt.executeQuery());
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public CloseableIterable<Edge> getEdges(String key, Object value) {
        return query().has(key, value).edges();
    }

    @Override
    public SqlGraphQuery query() {
        return new SqlGraphQuery(this);
    }

    @Override
    public void shutdown() {
        this.closeLocalConnection();
    }

    @Override
    public String toString() {
        return "sqlgraph()"; //TODO
    }

    
    Connection getConnection() {
        IResult c = getMapConnection();
        return (Connection)c.getResultObject();
    }

    Statements getStatements() {
        return statements;
    }

    String getVerticesTableName() {
        return verticesTableName;
    }

    String getEdgesTableName() {
        return edgesTableName;
    }

    String getVertexPropertiesTableName() {
        return vertexPropertiesTableName;
    }

    String getEdgePropertiesTableName() {
        return edgePropertiesTableName;
    }

    /*
    private Connection newConnection() throws SQLException {
        Connection conn = dataSource.getConnection();
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        return conn;
    }*/

    private String getId(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("null id");
        } 
        return (String)id;
    }

    private SqlVertex cache(SqlVertex v) {
        if (v != null) {
            vertexCache.put(v.getId(), new WeakReference<>(v));
        }

        return v;
    }
    
	/////////////////
	// connection handling
	////////////////

	private IResult getMapConnection() {
		synchronized (localMapConnection) {
			IResult result = new ResultPojo();
			try {
				Connection con = this.localMapConnection.get();
				//because we don't "setInitialValue", this returns null if nothing for this thread
				if (con == null) {
					con = provider.getConnection();
					System.out.println("GETMAPCONNECTION " + con);
					localMapConnection.set(con);
				}
				result.setResultObject(con);
			} catch (Exception e) {
				result.addErrorString(e.getMessage());
				if (environment != null)
					environment.logError(e.getMessage(), e);
			}
			return result;
		}
	}

	IResult closeLocalConnection() {
		IResult result = new ResultPojo();
		boolean isError = false;
		try {
			synchronized (localMapConnection) {
				Connection con = this.localMapConnection.get();
				if (con != null)
					con.close();
				localMapConnection.remove();
				//  localMapConnection.set(null);
			}
		} catch (SQLException e) {
			isError = true;
			result.addErrorString(e.getMessage());
		}
		if (!isError)
			result.setResultObject("OK");
		return result;
	}
}
