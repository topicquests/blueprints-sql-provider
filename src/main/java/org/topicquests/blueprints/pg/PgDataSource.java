/**
 * 
 */
package org.topicquests.blueprints.pg;

import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import org.postgresql.Driver;


/**
 * @author jackpark
 *
 */
public class PgDataSource implements XADataSource, DataSource, ConnectionPoolDataSource, Serializable {
	private static final long serialVersionUID = 1288136338451857771L;
	private transient PrintWriter logWriter;
	private int loginTimeout = 0; // default
    private String userName = "";
    private String passwordChars = "";
    private String url = "";
	/**
	 * 
	 */
	public PgDataSource(String userName, String password, String dbUrl, String graphName) {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.userName = userName;
		this.passwordChars = password;
		url = "jdbc:postgresql://"+dbUrl+/*":"+_dbPort+*/"/"+graphName;

	}

	/* (non-Javadoc)
	 * @see javax.sql.CommonDataSource#getLogWriter()
	 */
	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return logWriter;
	}

	/* (non-Javadoc)
	 * @see javax.sql.CommonDataSource#setLogWriter(java.io.PrintWriter)
	 */
	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		logWriter = out;
	}

	// 0 means no timeout
	/* (non-Javadoc)
	 * @see javax.sql.CommonDataSource#setLoginTimeout(int)
	 */
	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
		this.loginTimeout = seconds;
	}

	/* (non-Javadoc)
	 * @see javax.sql.CommonDataSource#getLoginTimeout()
	 */
	@Override
	public int getLoginTimeout() throws SQLException {
		return loginTimeout;
	}

	/* (non-Javadoc)
	 * @see javax.sql.CommonDataSource#getParentLogger()
	 */
	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see javax.sql.ConnectionPoolDataSource#getPooledConnection()
	 */
	@Override
	public PooledConnection getPooledConnection() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see javax.sql.ConnectionPoolDataSource#getPooledConnection(java.lang.String, java.lang.String)
	 */
	@Override
	public PooledConnection getPooledConnection(String user, String password) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see javax.sql.DataSource#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException {
		return getJdbcConnection(userName, passwordChars);
	}

	private Connection getJdbcConnection(String user, String password) throws SQLException {
        
        Properties info = new Properties();
        info.setProperty("user", user);
        if (password != null && !password.equals(""))
        	info.put("password", password);
        System.out.println("PgDataSource.getJdbcConnection "+url+" "+info);
        Connection conn = DriverManager.getConnection(url, info);
        conn.setAutoCommit(false);
        return conn;
    }

	/* (non-Javadoc)
	 * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
	 */
	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see javax.sql.XADataSource#getXAConnection()
	 */
	@Override
	public XAConnection getXAConnection() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see javax.sql.XADataSource#getXAConnection(java.lang.String, java.lang.String)
	 */
	@Override
	public XAConnection getXAConnection(String user, String password) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
     * Create a new char array and copy all the data. If the size of the byte
     * array is zero, the same array is returned.
     *
     * @param chars the char array (may be null)
     * @return a new char array
     */
    char[] cloneCharArray(char[] chars) {
        if (chars == null) {
            return null;
        }
        int len = chars.length;
        if (len == 0) {
            return chars;
        }
        char[] copy = new char[len];
        System.arraycopy(chars, 0, copy, 0, len);
        return copy;
    }
}
