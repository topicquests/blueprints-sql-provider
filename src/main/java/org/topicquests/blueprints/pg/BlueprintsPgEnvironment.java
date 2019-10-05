/**
 * 
 */
package org.topicquests.blueprints.pg;
import java.util.*;

import org.topicquests.blueprints.pg.api.IPgSchema;
import org.topicquests.pg.PostgresConnectionFactory;
import org.topicquests.support.RootEnvironment;

import com.tinkerpop.blueprints.impls.sql.SqlGraph;


/**
 * @author jackpark
 *
 */
public class BlueprintsPgEnvironment extends RootEnvironment {
	private Map<String, SqlGraph> graphs;
	private PostgresConnectionFactory provider;
	private GraphDSL dsl;

	/**
	 * 
	 */
	public BlueprintsPgEnvironment() {
		super("postgress-props.xml", "logger.properties");
		graphs = new HashMap<String, SqlGraph>();
		String dbName = getStringProperty("GraphDatabaseName");
		String schemaName = getStringProperty("GraphDatabaseSchema");
        logDebug("BlueprintsPgEnvironment "+dbName+" "+schemaName);
		provider = new PostgresConnectionFactory(dbName, schemaName);
		dsl = new GraphDSL(this);

	}

	
	/**
	 * Return a graph named by <code>graphName</code>
	 * @param graphName
	 * @return
	 */
	public SqlGraph getGraph(String graphName) {
		SqlGraph g = graphs.get(graphName);
		
		if (g == null) {		
			g = new SqlGraph(this, provider);
			graphs.put(graphName, g);
		}
        
        return g;
	}
	
	/**
	 * Return the DSL
	 * @return
	 */
	public GraphDSL getDSL() {
		return dsl;
	}
	
	/**
	 * Dangerous: Empty the contents of a graph in the database
	 * @param graphName
	 */
	//public void clearGraph(String graphName) {
		//TODO
	//}
	
	public void shutDown() {
		Iterator<String>itr = graphs.keySet().iterator();
		while (itr.hasNext()) {
			graphs.get(itr.next()).shutdown();
		}
	}
}
