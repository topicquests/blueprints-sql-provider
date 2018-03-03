/**
 * 
 */
package org.topicquests.blueprints.pg;
import java.util.*;

import org.topicquests.blueprints.pg.api.IPgSchema;
import org.topicquests.pg.PostgreSqlProvider;
import org.topicquests.pg.api.IPostgreSqlProvider;
import org.topicquests.support.RootEnvironment;

import com.tinkerpop.blueprints.impls.sql.SqlGraph;


/**
 * @author jackpark
 *
 */
public class BlueprintsPgEnvironment extends RootEnvironment {
	private Map<String, SqlGraph> graphs;
	/**
	 * 
	 */
	public BlueprintsPgEnvironment() {
		super("postgress-props.xml", "logger.properties");
		graphs = new HashMap<String, SqlGraph>();
	}

	
	/**
	 * Return a graph named by <code>graphName</code>
	 * @param graphName
	 * @return
	 */
	public SqlGraph getGraph(String graphName) {
		SqlGraph g = graphs.get(graphName);
		
		if (g == null) {
			IPostgreSqlProvider provider = new PostgreSqlProvider(graphName, "BlueprintsSchema");
			provider.validateDatabase(IPgSchema.SCHEMA);
		
			g = new SqlGraph(this, provider);
			graphs.put(graphName, g);
		}
        
        return g;
	}
	
	/**
	 * Dangerous: Empty the contents of a graph in the database
	 * @param graphName
	 */
	public void clearGraph(String graphName) {
		//TODO
	}
	
	public void shutDown() {
		Iterator<String>itr = graphs.keySet().iterator();
		while (itr.hasNext()) {
			graphs.get(itr.next()).shutdown();
		}
	}
}
