/**
 * 
 */
package devtests;

import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;
import org.topicquests.blueprints.pg.GraphDSL;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.sql.SqlGraph;

/**
 * @author jackpark
 *
 */
public class FindEdgeTest {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	private GraphDSL dsl;
	private final String
		V1 = "85defe40-dc4e-48e8-91cf-013ffc7c2a7e",
		V2 = "1e5bbc68-4f1e-42da-aad3-4f7936a706df",
		LBL = "My Second Edge";

	/**
	 * 
	 */
	public FindEdgeTest() {
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph("testgraph");
		dsl = environment.getDSL();
		try {
			Edge r = dsl.findEdgeByLabelAndVertices(V1, V2, LBL);
			System.out.println("DID "+r);
			if (r == null) {
				r = dsl.findEdgeByLabelAndVertices(V2, V1, LBL);
				System.out.println("AGAIN "+r);

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		environment.shutDown();
		System.exit(0);
		// TODO Auto-generated constructor stub
	}

}
