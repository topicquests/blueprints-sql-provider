/**
 * 
 */
package devtests;

import java.util.Iterator;
import java.util.UUID;

import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.sql.SqlGraph;

/**
 * @author jackpark
 *
 */
public class QueryTest {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	public final String
		ID			= UUID.randomUUID().toString(),
		ID2			= UUID.randomUUID().toString(),
		ID3			= UUID.randomUUID().toString(),
		EDGE_ID		= UUID.randomUUID().toString(),
		EDGE_ID2	= UUID.randomUUID().toString(),
		GRAPHNAME	= "bp6";

	/**
	 * 
	 */
	public QueryTest() {
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph(GRAPHNAME);
		Vertex v = graph.addVertex(ID, "A Funky Label"); //graph.addVertex(ID2);
		Vertex v2 = graph.addVertex(ID2, "Another Label");
		Vertex v3 = graph.addVertex(ID3, "Sup?");
		System.out.println("DID "+v+" "+v2);
		Edge e = graph.addEdge((Object)EDGE_ID, v, v2, "My First Edge");
		Edge e1 = graph.addEdge((Object)EDGE_ID2, v2, v3, "My Second Edge");
		System.out.println("AGAIN "+e+" "+e1);
		
		VertexQuery q = v.query();
		System.out.println("Q1 "+q.count());
		q = q.direction(Direction.OUT);
		Iterable<Edge> itbl = q.edges();
		Iterator<Edge> itr = itbl.iterator();
		Edge x;
		while (itr.hasNext()) {
			x = itr.next();
			System.out.println("Q2 ");
		}
		/*  the vertices() method seems to want to deal with fields only in edges
		Iterable<Vertex> ivx = q.vertices();
		Iterator<Vertex> itv = ivx.iterator();
		while (itv.hasNext()) {
			System.out.println("Q3 "+itv.next());
		}*/
		Iterable<Vertex> ifx = v.getVertices(Direction.BOTH, "Another Label");
		Iterator<Vertex> itv = ifx.iterator();
		while (itv.hasNext()) {
			System.out.println("Q3 "+itv.next());
		}
		environment.shutDown();
		System.exit(0);
	}

}
