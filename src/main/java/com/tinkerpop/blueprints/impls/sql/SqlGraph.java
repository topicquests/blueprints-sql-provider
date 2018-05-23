package com.tinkerpop.blueprints.impls.sql;

import java.lang.ref.WeakReference;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.WeakHashMap;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

import org.topicquests.blueprints.pg.BlueprintsVertexIterable;
import org.topicquests.blueprints.pg.BlueprintsEdgeIterable;
import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;
import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 * @author Lukas Krejci
 * @since 1.0
 * @author for changes: park
 */
public final class SqlGraph implements Graph {
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

	private PostgresConnectionFactory provider;
    private final String verticesTableName;
    private final String edgesTableName;
    private final String vertexPropertiesTableName;
    private final String edgePropertiesTableName;
    

    //TODO should this be an LRUCache?
    private final WeakHashMap<String, WeakReference<SqlVertex>> vertexCache = new WeakHashMap<>();

   public BlueprintsPgEnvironment getEnvironment() {
	   return environment;
   }

    public PostgresConnectionFactory getProvider() {
    	return provider;
    }
    

    public SqlGraph(BlueprintsPgEnvironment env, PostgresConnectionFactory p) {
    	environment = env;
    	provider = p;
    	verticesTableName = "tq_graph.vertices";
        edgesTableName = "tq_graph.edges";
        vertexPropertiesTableName = "tq_graph.vertex_properties";
        edgePropertiesTableName = "tq_graph.edge_properties";
    }
    
    /**
     * Shortcut to adding a property to a vertex without fetching the entire vertex
     * @param vertexId
     * @param key
     * @param value
     */
    public void addToVertexSetProperty(String vertexId, String key, String value) {
        String sql = "SELECT value FROM tq_graph.vertex_properties  WHERE " +
                "vertex_id = ? AND key = ? AND value = ?";
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);

        	Object [] vals = new Object[3];
        	vals[0] = vertexId;
        	vals[1] = key;
        	vals[2] = value;
        	conn.executeSelect(sql, r, vals);
            ResultSet rs = (ResultSet)r.getResultObject();
            if (rs != null) {
                    if (rs.next())
                    	return;
            }
            conn.beginTransaction(r);
            sql = "INSERT INTO tq_graph.vertex_properties (vertex_id, key, value) VALUES (?, ?, ?)";
            conn.executeSQL(sql, r, vals);
            conn.endTransaction(r);

        } catch (SQLException e) {
                throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }

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
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
	    Vertex result = null;
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);
        	conn.beginTransaction(r);
        	String sql = "INSERT INTO tq_graph.vertices (id, label) VALUES (?, ?)";
        	Object [] vals = new Object[2];
        	vals[0] = id;
        	vals[1] = label;
        	conn.executeSQL(sql, r, vals);
        	conn.endTransaction(r);
        	result = this.cache(new SqlVertex(this, id, label));
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }
        return result;
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

	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);
        	String sql = "SELECT id, label FROM tq_graph.vertices WHERE id = ?";
        	conn.executeSelect(sql, r, (String)id);
        	ResultSet rs = (ResultSet)r.getResultObject();
        	if (rs != null) {
        		if (rs.next()) {
        			String idx = rs.getString("id");
        			String lbl = rs.getString("label");
        			v = this.cache(new SqlVertex(this, idx, lbl));
        		}
        	}
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }
        return v;
    }

    @Override
    public void removeVertex(Vertex vertex) {
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);
        	conn.beginTransaction(r);

        	String sql = "DELETE FROM vertices WHERE id = ?";
        	conn.executeSQL(sql, r, (String)vertex.getId());
        	conn.endTransaction(r);
        	
            vertexCache.remove(vertex.getId());
            
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }
    }

    @Override
    public CloseableIterable<Vertex> getVertices() {
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
        	conn.setProxyRole(r);
            String sql = "SELECT id, label FROM tq_graph.vertices";
            conn.executeSelect(sql, r,  ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, null);
            ResultSet rs = (ResultSet)r.getResultObject();
            return (CloseableIterable<Vertex>)new BlueprintsVertexIterable(this, rs);

        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        return query().has(key, value).vertices();
    }

    @Override
    public SqlEdge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        if (label == null) {
            throw new IllegalArgumentException("null label");
        }

	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
	    SqlEdge result = null;
        try {
        	conn = provider.getConnection();
        	conn.setProxyRole(r);
        	conn.beginTransaction(r);
        	String sql = "INSERT INTO tq_graph.edges (id, vertex_in, vertex_out, label) VALUES (?, ?, ?, ?)";
        	Object [] vals = new Object[4];
        	vals[0] = id;
        	vals[1] = inVertex.getId();
        	vals[2] = outVertex.getId();
        	vals[3] = label;
        	conn.executeSQL(sql, r, vals);
        	conn.endTransaction(r);
        	result = new SqlEdge(this, (String) id, (String)inVertex.getId(),
        			(String)outVertex.getId(), label);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }
        return result;
    }

    @Override
    public SqlEdge getEdge(Object id) {
        String eid = getId(id);
        if (eid == null) {
            return null;
        }

	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
	    SqlEdge result = null;;
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);
        	String sql = "SELECT id, vertex_in, vertex_out, label FROM tq_graph.edges WHERE id = ?";	
        	conn.executeSelect(sql, r, id);
        	ResultSet rs = (ResultSet)r.getResultObject();
        	if (rs != null) {
        		if (rs.next()) {
        			result = new SqlEdge( this,
        					rs.getString(1),
        					rs.getString(2),
        					rs.getString(3),
        					rs.getString(4));
        		}
        	}
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }
        return result;
    }

    @Override
    public void removeEdge(Edge edge) {
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
            conn = provider.getConnection();
           	conn.setProxyRole(r);
            conn.beginTransaction(r);
            String sql = "DELETE FROM tq_graph.edges WHERE id = ?";
            //we are going to ignore removing something that doesn't exist
            conn.executeSQL(sql, r, edge.getId());
            conn.endTransaction(r);
         } catch (SQLException e) {
            throw new SqlGraphException(e);
         } finally {
 	    	conn.closeConnection(r);
         }
    }

    @Override
    public CloseableIterable<Edge> getEdges() {
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);
            String sql = "SELECT id, vertex_in, vertex_out, label FROM tq_graph.edges";
            conn.executeSelect(sql, r, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, null);
        	
            ResultSet rs = (ResultSet)r.getResultObject();
            return (CloseableIterable<Edge>)new BlueprintsEdgeIterable(this, rs);

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
       //
    }

    @Override
    public String toString() {
        return "sqlgraph()"; //TODO
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
    
}
