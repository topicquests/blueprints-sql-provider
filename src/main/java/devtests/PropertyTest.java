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
public class PropertyTest {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	public final String
		ID			= UUID.randomUUID().toString(),
		ID2			= UUID.randomUUID().toString(),
		EDGE_ID		= UUID.randomUUID().toString(),
		KEY			= "creatorId",
		CREATOR		= "JoeSixpack",
		GRAPHNAME	= "bp6";

	/**
	 * 
	 */
	public PropertyTest() {
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph(GRAPHNAME);
		Vertex v = graph.addVertex(ID2, "A Funky Label"); //graph.addVertex(ID2);
		v.setProperty(KEY, CREATOR);
		Vertex v2 = graph.addVertex(ID, "Another Label");
		v2.setProperty(KEY, CREATOR);
		System.out.println("DID "+v+" "+v2);
		Edge e = graph.addEdge((Object)EDGE_ID, v, v2, "My First Edge");
		e.setProperty(KEY, CREATOR);
		System.out.println("AGAIN "+e);
		
		v = graph.getVertex(ID);
		System.out.println("VP "+v.getProperty(KEY));
		
		e = graph.getEdge(EDGE_ID);
		System.out.println("EP "+e.getProperty(KEY));
		environment.shutDown();
		System.exit(0);
	}

}
