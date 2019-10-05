/**
 * 
 */
package devtests;

import java.util.UUID;

import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.sql.SqlGraph;

/**
 * @author jackpark
 *
 */
public class FirstDupEdgeTest {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	public final String
		ID			= UUID.randomUUID().toString(),
		ID2			= UUID.randomUUID().toString(),
		EDGE_ID		= UUID.randomUUID().toString(),
		GRAPHNAME	= "testgraph";
	/**
	 * 
	 */
	public FirstDupEdgeTest() {
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph(GRAPHNAME);
		Vertex v = graph.addVertex(ID2, "A Funky Label"); //graph.addVertex(ID2);
		Vertex v2 = graph.addVertex(ID, "Another Label");
		System.out.println("DID "+v+" "+v2);
		Edge e = graph.addEdge((Object)EDGE_ID, v, v2, "My Second Edge");
		System.out.println("AGAIN "+e);
		String x = EDGE_ID+"123";
		e = graph.addEdge((Object)x, v, v2, "My Second Edge");
		System.out.println("AGAIN+ "+e);
		
		environment.shutDown();
		System.exit(0);
	}

}
