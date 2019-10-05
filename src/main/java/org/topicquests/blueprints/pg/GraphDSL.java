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
	 * List the OUT vertices for a vertex with the given ID <code>vID</code>
	 * and the given edge label <code>eLabel</code>
	 * @param vID
	 * @param eLabel
	 * @return
	 * @throws Exception
	 */
	public List<JSONObject> listOutVertices(String vID, String eLabel) throws Exception {
		List<JSONObject> result = new ArrayList<JSONObject>();
		String sqle = "SELECT id, vertex_out, vertex_in, label FROM tq_graph.edges WHERE vertex_in=? AND label=?";
		Object [] o = new Object[2];
		o[0]= vID;
		o[1]= eLabel;
		IResult r = new ResultPojo();
		IPostgresConnection conn = runQuery(sqle, r, o);
		ResultSet rs = (ResultSet)r.getResultObject();
		System.out.println("AAA-- "+r.getErrorString());
		String eid, vout, vin, lab;
		if (rs != null) {
			while (rs.next()) {
				eid = rs.getString("id");
				vout = rs.getString("vertex_out");
				vin = rs.getString("vertex_in");
				lab = rs.getString("label");
				result.add(getVertex(vout));		
			}
		}
		conn.closeConnection(r);
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
		String sqle = "SELECT id, vertex_out, vertex_in, label FROM tq_graph.edges WHERE vertex_out=? AND label=?";
		Object [] o = new Object[2];
		o[0]= vID;
		o[1]= eLabel;
		IResult r = new ResultPojo();
		IPostgresConnection conn = runQuery(sqle, r, o);
		ResultSet rs = (ResultSet)r.getResultObject();
		System.out.println("AAA-- "+r.getErrorString());
		String eid, vout, vin, lab;
		if (rs != null) {
			while (rs.next()) {
				eid = rs.getString("id");
				vout = rs.getString("vertex_out");
				vin = rs.getString("vertex_in");
				lab = rs.getString("label");
				result.add(getVertex(vin));
			}
		}
		conn.closeConnection(r);
		return result;
	}
	
	JSONObject getVertex(String vID) throws Exception {
		JSONObject result = null;
		String sql = "SELECT label FROM tq_graph.vertices WHERE id=?";
		Object [] o = new Object[1];
		o[0]=vID;
		IResult r = new ResultPojo();
		IPostgresConnection conn = runQuery(sql, r, o);
		ResultSet rs = (ResultSet)r.getResultObject();
		if (rs != null && rs.next()) {
			result = new JSONObject();
			result.put("id", vID);
			result.put("label", rs.getString("label"));
		}
		conn.closeConnection(r);
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
