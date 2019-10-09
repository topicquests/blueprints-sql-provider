/**
 * 
 */
package org.topicquests.blueprints.pg;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.sql.SqlGraph;

import net.minidev.json.JSONObject;

/**
 * @author jackpark
 *
 */
public class GraphDSL {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	private PostgresConnectionFactory factory;

	/**
	 * 
	 */
	public GraphDSL(BlueprintsPgEnvironment env) {
		environment = env;
		graph = environment.getGraph("wordgrams");
		factory = graph.getProvider();
	}
	
	/**
	 * Find an edge by its primary parameters
	 * @param inVertexId
	 * @param outVertexId
	 * @param label
	 * @return can return <code>null</code>
	 * @throws Exception
	 */
	public Edge findEdgeByLabelAndVertices(String inVertexId,
										   String outVertexId,
										   String label) throws Exception {
		Edge result = null;
		String sql = "SELECT id FROM tq_graph.edges WHERE vertex_in = ? "+
				     "AND vertex_out=? AND label=?";
		Object [] o = new Object[3];
		o[0] = inVertexId;
		o[1] = outVertexId;
		o[2] = label;
		IResult r = new ResultPojo();
		IPostgresConnection conn = null;
		try {
			conn = runQuery(sql, r, o);
			ResultSet rs = (ResultSet)r.getResultObject();
			System.out.println("AAA-- "+r.getErrorString());
			if (rs != null) {
				if (rs.next()) {
					String eid = rs.getString("id");
					result = graph.getEdge(eid);
				}
			}		
		} finally {
			if (conn != null)
				conn.closeConnection(r);
		}
		return result;
	}

	/**
	 * List the OUT vertices for a vertex with the given ID <code>vID</code>
	 * and the given edge label <code>eLabel</code>
	 * @param vID
	 * @param eLabel
	 * @return
	 * @throws Exception
	 */
	public List<JSONObject> listOutVertices(String vID, String eLabel) throws Exception {
		List<JSONObject> result = new ArrayList<JSONObject>();
		String sqle = "SELECT vertex_outFROM tq_graph.edges WHERE vertex_in=? AND label=?";
		Object [] o = new Object[2];
		o[0]= vID;
		o[1]= eLabel;
		IResult r = new ResultPojo();
		IPostgresConnection conn = null;
		try {
			conn = runQuery(sqle, r, o);
			ResultSet rs = (ResultSet)r.getResultObject();
			System.out.println("AAA-- "+r.getErrorString());
			if (rs != null) {
				String vout;
				while (rs.next()) {
					vout = rs.getString("vertex_out");
					result.add(getVertex(vout));		
				}
			}
		} finally {
			if (conn != null)
				conn.closeConnection(r);
		}
		return result;
	}
	
	/**
	 * List the IN vertices for a vertex with the given ID <code>vID</code>
	 * and the given edge label <code>eLabel</code>
	 * @param vID
	 * @param eLabel
	 * @return
	 * @throws Exception
	 */
	public List<JSONObject> listInVertices(String vID, String eLabel) throws Exception {
		List<JSONObject> result = new ArrayList<JSONObject>();
		String sqle = "SELECT vertex_in FROM tq_graph.edges WHERE vertex_out=? AND label=?";
		Object [] o = new Object[2];
		o[0]= vID;
		o[1]= eLabel;
		IResult r = new ResultPojo();
		IPostgresConnection conn = null;
		try {
			conn = runQuery(sqle, r, o);
			ResultSet rs = (ResultSet)r.getResultObject();
			System.out.println("AAA-- "+r.getErrorString());
			if (rs != null) {
				String vin;
				while (rs.next()) {
					vin = rs.getString("vertex_in");
					result.add(getVertex(vin));
				}
			}
		} finally {
			if (conn != null)
				conn.closeConnection(r);
		}
		return result;
	}
	
	JSONObject getVertex(String vID) throws Exception {
		JSONObject result = null;
		String sql = "SELECT label FROM tq_graph.vertices WHERE id=?";
		Object [] o = new Object[1];
		o[0]=vID;
		IResult r = new ResultPojo();
		IPostgresConnection conn = null;
		try {
			conn = runQuery(sql, r, o);
			ResultSet rs = (ResultSet)r.getResultObject();
			if (rs != null && rs.next()) {
				result = new JSONObject();
				result.put("id", vID);
				result.put("label", rs.getString("label"));
			}
		} finally {
			if (conn != null)
				conn.closeConnection(r);
		}
		return result;
	}
	

	IPostgresConnection runQuery(String sql, IResult r, Object [] obj) throws Exception {
		IPostgresConnection conn = null;
		try {
			conn = factory.getConnection();
		    conn.setProxyRole(r);			
			conn.executeSelect(sql, r, obj);
		} catch (Exception e) {
			throw new Exception(e);
		}
		return conn;
	}
}
