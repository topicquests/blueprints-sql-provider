/**
 * 
 */
package devtests;

import java.sql.ResultSet;
import java.util.*;

import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;
import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.sql.SqlGraph;

import net.minidev.json.JSONObject;

/**
 * @author jackpark
 *
 */
public class VertexEdgeTest {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	private PostgresConnectionFactory factory;
	private final String VID = "9901.3850"; // African violet
	// has one out edge isA
	private final String ELABEL = "isA";
	private List<String> dupStopper;

	/**
	 * 
	 */
	public VertexEdgeTest() {
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph("wordgrams");
		factory = graph.getProvider();
		dupStopper = new ArrayList<String>();
		try {
			runTest();
		} catch (Exception e) {
			e.printStackTrace();
		}
		environment.shutDown();
		System.exit(0);
	}
	
	void runTest() throws Exception {
		List<JSONObject> result = listOutVertices(VID, ELABEL);
		System.out.println("XXX "+result);
		//XXX [{"id":"4681.","label":"flower"}]
		// which says that an Africon Violet isA flower
		// we can begin to run trees by recursively calling these routines
		// both up and down from some starter seed vId and isA
		
	}
	
	List<JSONObject> listOutVertices(String vID, String eLabel) throws Exception {
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
		dupStopper.clear();
		if (rs != null) {
			while (rs.next()) {
				eid = rs.getString("id");
				vout = rs.getString("vertex_out");
				vin = rs.getString("vertex_in");
				lab = rs.getString("label");
				//testing for in - watch for duplicate out
				if (!dupStopper.contains(vout)) {
					dupStopper.add(vout);
					result.add(getVertex(vout));
				}
				
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
	List<JSONObject> listInVertices(String vID, String eLabel) throws Exception {
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
		dupStopper.clear();
		if (rs != null) {
			while (rs.next()) {
				eid = rs.getString("id");
				vout = rs.getString("vertex_out");
				vin = rs.getString("vertex_in");
				lab = rs.getString("label");
				//testing for out - watch for duplicate in
				if (!dupStopper.contains(vin)) {
					dupStopper.add(vin);
				}
				result.add(getVertex(vin));
			}
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
