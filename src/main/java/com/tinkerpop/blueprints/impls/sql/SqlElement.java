package com.tinkerpop.blueprints.impls.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

import com.tinkerpop.blueprints.Element;

import net.minidev.json.JSONObject;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 * @author Lukas Krejci
 * @since 1.0
 */
abstract class SqlElement implements Element {

    protected final SqlGraph graph;
    private final String id;
    protected String label;
	protected PostgresConnectionFactory provider;


    protected SqlElement(SqlGraph graph, String id, String label) {
        if (id == null) {
            throw new IllegalArgumentException("id can't be null");
        }

        this.graph = graph;
        this.id = id;
        this.label = label;
        provider = graph.getProvider();
    }

    protected abstract String getPropertiesTableName();

    protected abstract String getPropertyTableElementIdName();

    protected abstract List<String> getDisallowedPropertyNames();
    
    protected abstract String getTableName();
    
    //property getters and setters serve both edges and vertices
    // thus they need to getPropertyTableName
    /**
     * This is intended only for singleton properties
     * But, just in case, it will still return a collection
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProperty(String key) {	//									e.g. vertex_properties
       T result = null;
    	String sql = "SELECT value FROM " + getPropertiesTableName() + " WHERE " +
            getPropertyTableElementIdName() + " = ? AND key = ?";
        	//e.g. vertex_id
//        graph.getEnvironment().logDebug("SqlElement.getProperty- "+key+" "+sql);
      	IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);
//            graph.getEnvironment().logDebug("SqlElement.getProperty-1 "+conn);

        	Object [] vals = new Object[2];
        	vals[0] = id;
        	vals[1] = key;
        	conn.executeSelect(sql, r, vals);
            ResultSet rs = (ResultSet)r.getResultObject();
//            graph.getEnvironment().logDebug("SqlElement.getProperty-2 "+key+" "+rs);
            if (rs != null) {
            	//MODIFY to return String or collection
            	//Required modification of property tables to
            	// drop UNIQUE in favor of PRIMARY KEY
                String val = null;
                List<String> valx= null;
                while (rs.next()) {
                	if (val == null)
                		val = rs.getString("value");
                	else if (valx == null) {
                		valx = new ArrayList<String>();
                		valx.add(val);
                		valx.add(rs.getString("value"));
                	} else {
                		valx.add(rs.getString("value"));
                	}
                }
    	        if (valx != null)
                	result = (T) valx;
                else
                	result =  (T) val;
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
        	conn.closeConnection(r);
        }
//        graph.getEnvironment().logDebug("SqlElement.getProperty+ "+key+" "+result);
        return result;
    }
    
    /**
     * Can return an empty list
     * @param key
     * @return
     */
    public List<String> listProperty(String key) {
    	List<String> result = new ArrayList<String>();
        String sql = "SELECT value FROM " + getPropertiesTableName() + " WHERE " +
                getPropertyTableElementIdName() + " = ? AND key = ?";
            	//e.g. vertex_id
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);

        	Object [] vals = new Object[2];
        	vals[0] = id;
        	vals[1] = key;
        	conn.executeSelect(sql, r, vals);
            ResultSet rs = (ResultSet)r.getResultObject();
            if (rs != null) {
                    while (rs.next()) {
                    	result.add(rs.getString("value"));
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
    public Set<String> getPropertyKeys() {
    	Set<String> result = null;
        String sql =
            "SELECT key FROM " + getPropertiesTableName() + " WHERE " + getPropertyTableElementIdName() + " = ?";
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);

        	conn.executeSelect(sql, r, id);
 
            Set<String> ret = new HashSet<>();
            ResultSet rs = (ResultSet)r.getResultObject();
            if (rs != null) {
                    while (rs.next()) {
                    	ret.add(rs.getString(1));
                    }
            }
            result = ret;
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
        	conn.closeConnection(r);
        }
        return result;
    }

    public void updateProperty(String key, String newValue, String oldValue) {
    	String sql = "UPDATE " + getPropertiesTableName() + " SET value" +
                " = ? WHERE " +
                getPropertyTableElementIdName() + " = ? AND key = ? AND value = ?";
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();
           	conn.setProxyRole(r);

        	Object [] vals = new Object[4];     		
			vals[0] = newValue;
			vals[1] = id;
			vals[2] = key;
			vals[3] = oldValue;
			conn.executeUpdate(sql, r, vals);

        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
	    	conn.closeConnection(r);
        }
    }
    
    /**
     * Builds non-redundant lists of properties
     * If you don't mind redundant entries, just use setProperty
     * @param key
     * @param value
     */
    public void addToSetProperty(String key, String value) {
    	List<String>l = this.listProperty(key);
    	if (l.isEmpty() || !l.contains(value))
    		setProperty(key, value);
    }

    @Override
    public void setProperty(String key, Object value) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("empty key");
        }

        if (getDisallowedPropertyNames().contains(key)) {
            throw new IllegalArgumentException("disallowed property name");
        }

        if (value == null) {
            throw new IllegalArgumentException("null value not allowed");
        }
        String sql = "INSERT INTO " + getPropertiesTableName() + " (" + getPropertyTableElementIdName() +
            ", key, value) VALUES (?, ?, ?)";

	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
	    try {
	    	conn = provider.getConnection();
	       	conn.setProxyRole(r);

	    	conn.beginTransaction(r);
	    	Object [] vals = new Object[3];
	    	vals[0] = id;
	    	vals[1] = key;
	    	vals[2] = value;
	    	conn.executeSQL(sql, r, vals);
	    	conn.endTransaction(r);
	    	
         } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
        	conn.closeConnection(r);
        }
    }

    /**
     * Delete a specific key/value pair
     * @param key
     * @param value
     */
    public void deleteProperty(String key, String value) {
        String sql = "DELETE FROM " + getPropertiesTableName() + " WHERE " + getPropertyTableElementIdName() + " = ? AND key = ? AND value = ?";
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();   
           	conn.setProxyRole(r);

	    	conn.beginTransaction(r);
	    	Object [] vals = new Object[3];
	    	vals[0] = id;
	    	vals[1] = key;
	    	vals[2] = value;
	    	conn.executeSQL(sql, r, vals);
	    	conn.endTransaction(r);
	    } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
        	conn.closeConnection(r);
        }
    }
    
    @Override
    public <T> T removeProperty(String key) {
        T value = getProperty(key);

        String sql = "DELETE FROM " + getPropertiesTableName() + " WHERE " + getPropertyTableElementIdName() + " = ? AND key = ?";
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection(); 
           	conn.setProxyRole(r);

	    	conn.beginTransaction(r);
	    	Object [] vals = new Object[2];
	    	vals[0] = id;
	    	vals[1] = key;
	    	conn.executeSQL(sql, r, vals);
	    	conn.endTransaction(r);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
        	conn.closeConnection(r);
        }
        return value;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SqlElement sqlVertex = (SqlElement) o;

        if (!id.equals(sqlVertex.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append("[id=").append(id);
        sb.append(", label=").append(label);
        sb.append(']');
        return sb.toString();
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) throws SqlGraphException {
    	this.label = label;
        String sql = "UPDATE " + getTableName() + " SET label" +
                " = ? WHERE id = ?";
	    IPostgresConnection conn = null;
	    IResult r = new ResultPojo();
        try {
        	conn = provider.getConnection();   
           	conn.setProxyRole(r);

	    	conn.beginTransaction(r);
	    	Object [] vals = new Object[2];
	    	vals[0] = label;
	    	vals[1] = getId();
	    	conn.executeSQL(sql, r, vals);
	    	conn.endTransaction(r);
	    	
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        } finally {
        	conn.closeConnection(r);
        }
    }
    
    /**
     * Return this object as a JSONObject
     * @return
     */
    public JSONObject getData() {
    	//TODO this is really slow--should be put inside a connection
    	JSONObject result = new JSONObject();
    	result.put("id", getId());
    	result.put("label", getLabel());
    	Iterator<String>itr = this.getPropertyKeys().iterator();
    	String key;;
    	while (itr.hasNext()) {
    		key = itr.next();
    		if (!key.equals("label")) {
    			result.put(key, getProperty(key));
    		}
    	}
    	return result;
    }
}
