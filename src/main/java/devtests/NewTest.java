/**
 * 
 */
package devtests;

import java.util.Iterator;
import java.util.UUID;

import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.impls.sql.SqlEdge;
import com.tinkerpop.blueprints.impls.sql.SqlGraph;
import com.tinkerpop.blueprints.impls.sql.SqlVertex;

/**
 * @author jackpark
 * Jacks-MacBook-Pro:config jackpark$ psql -U postgres
psql (9.6.5)
Type "help" for help.

postgres=# CREATE DATABASE graphtest ENCODING 'UTF-8';
CREATE DATABASE
postgres=# \c graphtest
You are now connected to database "graphtest" as user "postgres".
graphtest=# \i graph.sql

 */
public class NewTest {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	public final String
		ID			= UUID.randomUUID().toString(),
		ID2			= UUID.randomUUID().toString(),
		ID3			= UUID.randomUUID().toString(),
		EDGE_ID		= UUID.randomUUID().toString(),
		EDGE_ID2	= UUID.randomUUID().toString(),
		DBNAME		= "graphtest",
		GRAPHNAME	= "tq_graph";

	/**
	 * 
	 */
	public NewTest() {
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph(GRAPHNAME);
		System.out.println("A "+graph);
		showVertexIterator();
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
		SqlEdge y;
		while (itr.hasNext()) {
			x = itr.next();
			System.out.println("Q2 "+((SqlEdge)x).getLabel());
		}
		/*  the vertices() method seems to want to deal with fields only in edges
		Iterable<Vertex> ivx = q.vertices();
		Iterator<Vertex> itv = ivx.iterator();
		while (itv.hasNext()) {
			System.out.println("Q3 "+itv.next());
		}*/
		Iterable<Vertex> ifx = v.getVertices(Direction.BOTH, "Another Label");
		Iterator<Vertex> itv = ifx.iterator();
		
		SqlVertex sv;
		while (itv.hasNext()) {
			sv = (SqlVertex)itv.next();
			System.out.println("Q3 "+sv.getLabel());
		}
		showVertexIterator();
		environment.shutDown();
		System.exit(0);
	}

	void showVertexIterator() {
		CloseableIterable<Vertex> v = graph.getVertices();
		Iterator<Vertex> itr = v.iterator();
		SqlVertex sv;
		while (itr.hasNext()) {
			sv = (SqlVertex)itr.next();
			System.out.println("ITR "+sv.getId());
		}
	}
}
