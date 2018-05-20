/**
 * 
 */
package org.topicquests.blueprints.pg;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.impls.sql.SqlEdge;
import com.tinkerpop.blueprints.impls.sql.SqlGraph;
import com.tinkerpop.blueprints.impls.sql.SqlGraphException;

/**
 * @author jackpark
 *
 */
public class BlueprintsEdgeIterable<T> implements CloseableIterable<T> {
	private ResultSet rs;
	private SqlGraph graph;

	/**
	 * 
	 */
	public BlueprintsEdgeIterable(SqlGraph g, ResultSet r) {
		graph = g;
		rs = r;
	}

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<T> iterator() {
        try {
        	if (rs != null)
        		rs.beforeFirst();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }

        return new Iterator<T>() {
			T next;
	        long cnt;
            @Override
            public boolean hasNext() {
            	if (rs == null)
            		return false;
            	next = next();
            	return (next != null);
            }
            @Override
            public T next() {
            	if (next != null) {
            		T temp = next;
            		next = null;
            		return temp;
            	}
                try {
                    advance();
                    return next;
                } finally {
                    next = null;
                }
            }
            
            private void advance() {
                if (next == null) {
                    try {
                        if (rs.next()) {
                            next = (T)new SqlEdge( graph,
                            		rs.getString(1),
                            		rs.getString(2),
                            		rs.getString(3),
                            		rs.getString(4));
                            cnt++;
                        } else {
                        	close();
                        }
                    } catch (SQLException e) {
                        throw new SqlGraphException(e);
                    }
                }
            }
		};	
	}

	/* (non-Javadoc)
	 * @see com.tinkerpop.blueprints.CloseableIterable#close()
	 */
	@Override
	public void close() {
        try {
        	if (rs != null)
        		rs.close();
        } catch (SQLException e) {
            throw new SqlGraphException(e);
        }
	}

}
