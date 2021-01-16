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
     * Return a boolean <code>true</code> if a vertext identified by 
     * <code>vertexId</code>  exists
     * @param conn  caller responsible for closing
     * @param vertexId
     * @return
     */
    public boolean vertexExists(IPostgresConnection conn, String vertexId) {
	    String sql = "SELECT id FROM tq_graph.vertices WHERE id=?";
        try {
        	IResult r = conn.executeSelect(sql, vertexId);
        	ResultSet rs = (ResultSet)r.getResultObject();
        	if (rs != null && rs.next())
        		return true;
        	
        } catch (Exception e) {
        	environment.logError(e.getMessage(), e);
        }
        return false;
    }
    
    /**
     * Simple property value fetch
     * @param conn
     * @param id
     * @param key
     * @return
     */
    public String getVertexProperty(IPostgresConnection conn, String id, String key) {
    	String result = null;
    	String sql = "SELECT value FROM tq_graph.vertex_properties WHERE vertex_id = ? AND key = ?";
    	Object [] vals = new Object [2];
    	vals[0] = id;
    	vals[1] = key;
        try {
        	IResult r = conn.executeSelect(sql, vals);
        	ResultSet rs = (ResultSet)r.getResultObject();
        	if (rs != null) {
        		if (rs.next())
        			result = rs.getString(1);
        	}
        	
        } catch (Exception e) {
        	environment.logError(e.getMessage(), e);
        }
   	
    	
    	return result;
    }
    /**
     * Shortcut to adding a property to a vertex without fetching the entire vertex
     * @param vertexId
     * @param key
     * @param value
     */
    public IResult addToVertexSetProperty(String vertexId, String key, String value) {
	    IResult result = new ResultPojo();
    	IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);
            conn.beginTransaction(r);
        	addToVertexSetProperty(conn, vertexId, key, value, r);
            conn.endTransaction(r);
        } catch (Exception e) {
        	result.addErrorString(e.getMessage());
        	environment.logError(e.getMessage(), e);
            //    throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }
        return result;
    }

    public void addToVertexSetProperty(IPostgresConnection conn, String vertexId, 
    		String key, String value, IResult r) throws Exception {
    	Object [] vals = new Object[3];
    	vals[0] = vertexId;
    	vals[1] = key;
    	vals[2] = value;
    	String sql = "SELECT * FROM tq_graph.vertex_properties where vertex_id=? AND key=? AND value=?";
        String sql1 = "INSERT INTO tq_graph.vertex_properties (vertex_id, key, value) VALUES (?, ?, ?) ON CONFLICT DO NOTHING";
        
        conn.executeSelect(sql, r, vals);
        ResultSet rs = (ResultSet)r.getResultObject();
        environment.logDebug("SqlGraph.addToVertexSetProperty "+vertexId+" "+key+" "+value+" "+rs);
        if (rs == null || !rs.next()) {
           environment.logDebug("SqlGraph.addToVertexSetProperty-1 ");
           conn.executeSQL(sql1, r, vals);
        }

    }

    public void addToEdgeSetProperty(IPostgresConnection conn, String edgeId, 
    		String key, String value, IResult r) throws Exception {
    	conn.beginTransaction(r);
    	Object [] vals = new Object[3];
    	vals[0] = edgeId;
    	vals[1] = key;
    	vals[2] = value;
    	String sql = "SELECT * FROM tq_graph.edge_properties where edge_id=? AND key=? AND value=?";
        String sql1 = "INSERT INTO tq_graph.edge_properties (edge_id, key, value) VALUES (?, ?, ?) ON CONFLICT DO NOTHING";
        conn.executeSelect(sql, r, vals);
        ResultSet rs = (ResultSet)r.getResultObject();
        if (rs == null || !rs.next())
            conn.executeSQL(sql1, r, vals);
        conn.endTransaction(r);
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
        	result = addVertex(conn, id, label, r);
        	conn.endTransaction(r);
        } catch (Exception e) {
        	environment.logError(e.getMessage(), e);
            throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }
        return result;
    }
    
    public Vertex addVertex(IPostgresConnection conn, String id, String label, IResult r) throws Exception {
    	Vertex result = null;
    	String sql = "INSERT INTO tq_graph.vertices (id, label) VALUES (?, ?)";
    	Object [] vals = new Object[2];
    	vals[0] = id;
    	vals[1] = label;
       	conn.executeSQL(sql, r, vals);
        result = this.cache(new SqlVertex(this, id, label));
	    return result;
    }

    @Override
    public SqlVertex getVertex(Object id) {

    	SqlVertex v = null;
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);
           	v = getVertex(id, conn, r);
        } catch (Exception e) {
            throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }
        return v;
    }
    
    public SqlVertex getVertex(Object id, IPostgresConnection conn, IResult r) throws Exception {
        String realId = getId(id);

        if (realId == null) {
            return null;
        }

        WeakReference<SqlVertex> ref = vertexCache.get(realId);

        SqlVertex v = ref == null ? null : ref.get();
        if (v != null) {
            return v;
        }	
        
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

    public SqlEdge addEdge(IPostgresConnection conn, Object id, String outVertexId, String inVertexId, 
    		String label, IResult r) throws Exception {
        if (label == null) {
            throw new IllegalArgumentException("null label");
        }
    	conn.beginTransaction(r);
    	String sql = "INSERT INTO tq_graph.edges (id, vertex_in, vertex_out, label) VALUES (?, ?, ?, ?)";
    	Object [] vals = new Object[4];
    	vals[0] = id;
    	vals[1] = inVertexId;
    	vals[2] = outVertexId;
    	vals[3] = label;
    	conn.executeSQL(sql, r, vals);
    	if (r.hasError()) {
    		environment.logError("SqlGraph.addEdge "+r.getErrorString(), null);
    		return null;
    	}
    	SqlEdge result = new SqlEdge(this, (String) id, inVertexId,
    			outVertexId, label);   	
    	return result;
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
        	if (!r.hasError()) {
            	result = new SqlEdge(this, (String) id, (String)inVertex.getId(),
            			(String)outVertex.getId(), label);
        	}
        	conn.endTransaction(r);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }
        return result;
    }
    
    /**
     * Return <code>true</code> if the given edge already exists
     * @param conn
     * @param inVertexId
     * @param outVertexId
     * @param relationLabel
     * @return
     * @throws Exception
     */
    public boolean edgeExists(IPostgresConnection conn, String inVertexId, String outVertexId, String relationLabel)
    				throws Exception {
    	String sql = "SELECT id FROM tq_graph.edges WHERE label =?"+
    				"AND vertex_in=? AND vertex_out=?";
    	IResult r = new ResultPojo();
    	conn.setProxyRole(r);
    	Object [] obj = new Object[3];
    	obj[0] = relationLabel;
    	obj[1] = inVertexId;
    	obj[2] = outVertexId;
    	 
    	conn.executeSelect(sql, r, obj);
    	ResultSet rs = (ResultSet)r.getResultObject();
    	return (rs != null && rs.next());
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
