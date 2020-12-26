/**
 * 
 */
package devtests;

import java.sql.ResultSet;
import java.util.*;
import java.io.*;

import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;
import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.pg.api.IPostgresConnection;
import org.topicquests.support.ResultPojo;
import org.topicquests.support.api.IResult;

import com.tinkerpop.blueprints.impls.sql.SqlGraph;

import net.minidev.json.JSONObject;

/**
 * @author jackpark
 * @author Marc-Antoine Parent
 */
public class NounVertexTest {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	private PostgresConnectionFactory factory;
	private final String sql;
	private final int LIMIT = 300;
	private int offset = 0;
	private PrintWriter out;

	/**
	 * 
	 */
	public NounVertexTest() {
		System.out.println("Starting NounVertexTest");
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph("wordgrams");
		factory = graph.getProvider();
		try {
			File f = new File(Long.toString(System.currentTimeMillis())+".json");
			out = new PrintWriter(f, "UTF-8");


		sql = "SELECT tq_graph.vertices.id as vid, "+
				"    tq_graph.vertices.label as vlabel, "+
				"    min(tq_graph.vertex_properties.value) as lextype, "+
				"    tq_graph.edges.label as elabel, "+
				"    case when tq_graph.edges.vertex_in = tq_graph.vertices.id then 'out' else 'in' END as edir, "+
				"    count(tq_graph.edges.id) as ecount "+
				"FROM tq_graph.edges "+
				"   JOIN tq_graph.vertices ON (tq_graph.edges.vertex_in = tq_graph.vertices.id OR tq_graph.edges.vertex_out = tq_graph.vertices.id) "+
				"   JOIN tq_graph.vertex_properties ON tq_graph.vertex_properties.vertex_id=tq_graph.vertices.id "+
				"   AND tq_graph.vertex_properties.key='lexTypes' "+
				"    WHERE tq_graph.vertices.id in ( "+
				"        SELECT id FROM tq_graph.vertices "+
				"        JOIN tq_graph.vertex_properties ON tq_graph.vertex_properties.vertex_id=tq_graph.vertices.id "+
				"        WHERE tq_graph.vertex_properties.key='lexTypes' "+
				"        AND tq_graph.vertex_properties.value IN ('np', 'n') "+
				"        ORDER BY tq_graph.vertices.label LIMIT ? OFFSET ? "+
				"    ) "+
				"   GROUP BY tq_graph.vertices.id, tq_graph.edges.label, case when tq_graph.edges.vertex_in = tq_graph.vertices.id then 'out' else 'in' END ";
				
			IPostgresConnection conn = null;
			IResult r = new ResultPojo();
			try {
				conn = factory.getConnection();
			    conn.setProxyRole(r);
				Object [] obj = new Object[2];
				obj[0] = LIMIT;
				obj[1] = offset;
				conn.executeSelect(sql, r, obj);
				System.out.println("AAA-");
				ResultSet rs = (ResultSet)r.getResultObject();
				System.out.println("AAA-- "+r.getErrorString());

				String vid = null, vix, elbl, dir;
				JSONObject jo = null;
				List<String> outEdges = null, inEdges = null;
				
				if (rs != null ) {
					System.out.println("AAA");
					while (rs.next()) {
						System.out.println("BBB");

						if (vid == null) { // first pass
							jo = new JSONObject();
							vid =rs.getString("vid"); //(1); //vertex.id
							jo.put("vid", vid);
							inEdges = new ArrayList<String>();
							outEdges = new ArrayList<String>();
							jo.put("outEdges", outEdges);
							jo.put("inEdges", inEdges);
							jo.put("vlbl", rs.getString("vlabel"));
							//jo.put("lexType", rs.getString("lextype"));
						} else { // all subsequent passes
							vix = rs.getString("vid");
							
							if (!vix.equals(vid)) {
								out.println(jo.toJSONString());
								jo = new JSONObject();
								vid =rs.getString("vid");
								jo.put("vid", vid);
								inEdges = new ArrayList<String>();
								outEdges = new ArrayList<String>();
								jo.put("outEdges", outEdges);
								jo.put("inEdges", inEdges);
								jo.put("vlbl", rs.getString("vlabel"));
					//			jo.put("lexType", rs.getString("lextype"));	
							}
						}
						elbl = rs.getString("elabel");
						dir = rs.getString("edir");
						if (dir.equals("out")) {
							if (!outEdges.contains(elbl))
								outEdges.add(elbl);
						} else {
							if (!inEdges.contains(elbl))
								inEdges.add(elbl);
						}
					}
					// print the last one
					if (jo != null)
						out.println(jo.toJSONString());
				}
				
				out.flush();
				out.close();
				
	
			} catch (Exception e) {
				environment.logError(e.getMessage(), e);
				e.printStackTrace();
			} finally {
				conn.closeConnection(r);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		environment.shutDown();
	}

	
}
