/**
 * 
 */
package devtests;

import java.util.List;

import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;
import org.topicquests.blueprints.pg.GraphDSL;

import com.tinkerpop.blueprints.impls.sql.SqlGraph;

import net.minidev.json.JSONObject;

/**
 * @author jackpark
 *
 */
public class FirstDSLTest {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	private GraphDSL dsl;
	private final String
		V1 = "85defe40-dc4e-48e8-91cf-013ffc7c2a7e",
		V2 = "1e5bbc68-4f1e-42da-aad3-4f7936a706df",
		LBL = "My Second Edge";
/**
edgeId: 2ba3b746-f1fa-41a8-9766-d1f5f05c4ba0 
vertex-out: 85defe40-dc4e-48e8-91cf-013ffc7c2a7e 
vertex-in: 1e5bbc68-4f1e-42da-aad3-4f7936a706df 
edgeLabel: My Second Edge

V1 is an OUT for this edge so it will have V2 as an IN
V2 is an IN for this edge so it will have V1 as an OUT
 */
	/**
	 * 
	 */
	public FirstDSLTest() {
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph("testgraph");
		dsl = environment.getDSL();
		try {
			List<JSONObject> l = dsl.listInVertices(V1, LBL);
			System.out.println("A "+l);
			l = dsl.listOutVertices(V1, LBL);
			System.out.println("B "+l);
			l = dsl.listInVertices(V2, LBL);
			System.out.println("C "+l);
			l = dsl.listOutVertices(V2, LBL);
			System.out.println("D "+l);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		environment.shutDown();
		System.exit(0);

	}

}
/**
A [{"id":"1e5bbc68-4f1e-42da-aad3-4f7936a706df","label":"Another Label"}]
AAA-- 
B []
AAA-- 
C []
AAA-- 
D [{"id":"85defe40-dc4e-48e8-91cf-013ffc7c2a7e","label":"A Funky Label"}]
 */
