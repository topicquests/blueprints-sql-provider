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
public class PropertyTest_2 {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	public final String
		ID			= UUID.randomUUID().toString(),
		ID2			= UUID.randomUUID().toString(),
		EDGE_ID		= UUID.randomUUID().toString(),
		KEY			= "creatorId",
		CREATOR		= "JoeSixpack",
		GRAPHNAME	= "bp8";

	/**
	 * 
	 */
	public PropertyTest_2() {
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph(GRAPHNAME);
		Vertex v = graph.addVertex(ID, "A Funky Label"); //graph.addVertex(ID2);
		v.setProperty(KEY, CREATOR);
		v.setProperty(KEY, "foo");
		v.setProperty(KEY, "bar");
		v = graph.getVertex(ID);
		System.out.println("VP "+v.getProperty(KEY));
		//VP [JoeSixpack, foo, bar]

	
		environment.shutDown();
		System.exit(0);	}

}
