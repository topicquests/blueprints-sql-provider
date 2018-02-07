package com.tinkerpop.blueprints.impls.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tinkerpop.blueprints.Element;

/**
 * @author Mike Bryant (http://github.com/mikesname)
 * @author Lukas Krejci
 * @since 1.0
 */
abstract class SqlElement implements Element {

    protected final SqlGraph graph;
    private final String id;
    protected final String label;

    protected SqlElement(SqlGraph graph, String id, String label) {
        if (id == null) {
            throw new IllegalArgumentException("id can't be null");
        }

        this.graph = graph;
        this.id = id;
        this.label = label;
    }

    protected abstract String getPropertiesTableName();

    protected abstract String getPropertyTableElementIdName();

    protected abstract List<String> getDisallowedPropertyNames();
    
    protected abstract String getTableName();
    
    //property getters and setters serve both edges and vertices
    // thus they need to getPropertyTableName
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProperty(String key) {	//									e.g. vertex_properties
        String sql = "SELECT value FROM " + getPropertiesTableName() + " WHERE " +
            getPropertyTableElementIdName() + " = ? AND key = ?";
        	//e.g. vertex_id
        try (PreparedStatement stmt = graph.getConnection().prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setString(2, key);
            try (ResultSet rs = stmt.executeQuery()) {
            	//MODIFY to return String or collection
            	//Required modification of property tables to
            	// drop UNIQUE in favor of PRIMARY KEY
                String val = null;
                List<String> vals= null;
                while (rs.next()) {
                	if (val == null)
                		val = rs.getString("value");
                	else if (vals == null) {
                		vals = new ArrayList<String>();
                		vals.add(val);
                		vals.add(rs.getString("value"));
                	} else {
                		vals.add(rs.getString("value"));
                	}
                }
                if (vals != null)
                	return (T) vals;
                else
                	return (T) val;
            }
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    @Override
    public Set<String> getPropertyKeys() {
        String sql =
            "SELECT key FROM " + getPropertiesTableName() + " WHERE " + getPropertyTableElementIdName() + " = ?";

        try (PreparedStatement stmt = graph.getConnection().prepareStatement(sql)) {
            stmt.setString(1, id);

            Set<String> ret = new HashSet<>();

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    ret.add(rs.getString(1));
                }
            }

            return ret;
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    public void updateProperty(String key, String newValue, String oldValue) {
    	String sql = "UPDATE " + getPropertiesTableName() + " SET value" +
                " = ? WHERE " +
                getPropertyTableElementIdName() + " = ? AND key = ? AND value = ?";
        try (PreparedStatement stmt = graph.getConnection().prepareStatement(sql)) {
            stmt.setObject(1, newValue);
            stmt.setString(2, id);
            stmt.setString(3, key);
            stmt.setString(4, oldValue);
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
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

        try (PreparedStatement stmt2 = graph.getConnection().prepareStatement(sql)) {
            stmt2.setString(1, id);
            stmt2.setString(2, key);
            stmt2.setObject(3, value);

            stmt2.executeUpdate();
         } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }

    /**
     * Delete a specific key/value pair
     * @param key
     * @param value
     */
    public void deleteProperty(String key, String value) {
        String sql = "DELETE FROM " + getPropertiesTableName() + " WHERE " + getPropertyTableElementIdName() + " = ? AND key = ? AND value = ?";
        try (PreparedStatement stmt = graph.getConnection().prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setString(2, key);
            stmt.setString(3, value);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
    }
    
    @Override
    public <T> T removeProperty(String key) {
        T value = getProperty(key);

        String sql = "DELETE FROM " + getPropertiesTableName() + " WHERE " + getPropertyTableElementIdName() + " = ? AND key = ?";
        try (PreparedStatement stmt = graph.getConnection().prepareStatement(sql)) {
            stmt.setString(1, id);
            stmt.setString(2, key);
            stmt.executeUpdate();
            return value;
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
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
        String sql = "UPDATE " + getTableName() + " SET label" +
                " = ? WHERE id = ?";
        	Connection conn = graph.getConnection();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, label);
                stmt.setString(2, getId());
                stmt.executeUpdate();
            } catch (SQLException e) {
            	throw new SqlGraphException(e);
            }
    }
}
