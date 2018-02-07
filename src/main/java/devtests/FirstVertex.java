/**
 * 
 */
package devtests;

import java.util.Iterator;

import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;

import com.tinkerpop.blueprints.impls.sql.SqlGraph;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author jackpark
 *
 */
public class FirstVertex {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	public final String
		ID			= Long.toString(System.currentTimeMillis()),
		GRAPHNAME	= "bp4";

	/**
	 * 
	 */
	public FirstVertex() {
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph(GRAPHNAME);
		Vertex v = graph.addVertex(ID);
		System.out.println("DID "+v);
		CloseableIterable<Vertex> verts = graph.getVertices();
		Iterator<Vertex> itr = verts.iterator();
		
		while (itr.hasNext()) {
			v = itr.next();
			System.out.println("v "+v);
		}
				
		environment.shutDown();
		System.exit(0);
	}

}
