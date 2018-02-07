/**
 * 
 */
package devtests;

import org.topicquests.blueprints.pg.BlueprintsPgEnvironment;
import org.topicquests.support.api.IResult;

import com.tinkerpop.blueprints.impls.sql.SqlGraph;

/**
 * @author jackpark
 *
 */
public class FirstTest {
	private BlueprintsPgEnvironment environment;
	private SqlGraph graph;
	public final String
		GRAPHNAME			= "bp4";
	
	/**
	 * 
	 */
	public FirstTest() {
		environment = new BlueprintsPgEnvironment();
		graph = environment.getGraph(GRAPHNAME);
		System.out.println("DID "+graph);
		environment.shutDown();
		System.exit(0);
	}

}
