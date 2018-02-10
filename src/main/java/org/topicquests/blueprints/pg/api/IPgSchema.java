/**
 * 
 */
package org.topicquests.blueprints.pg.api;

/**
 * @author jackpark
 *
 */
public interface IPgSchema {
	
	public static final String [] SCHEMA = {
			"CREATE TABLE IF NOT EXISTS vertices ("+
			  "id text NOT NULL PRIMARY KEY,"+
			  "label text NOT NULL"+
			");",

			"CREATE TABLE IF NOT EXISTS edges ("+
			  "id text NOT NULL PRIMARY KEY,"+
			  "vertex_out text NOT NULL,"+
			  "vertex_in text NOT NULL,"+
			  "label text NOT NULL,"+
			  "CONSTRAINT fk_vertex_out FOREIGN KEY (vertex_out) REFERENCES vertices (id)"+
			   " ON DELETE CASCADE,"+
			  "CONSTRAINT fk_vertex_in FOREIGN KEY (vertex_in) REFERENCES vertices (id)"+
			    "ON DELETE CASCADE"+
			");",

			"CREATE INDEX IF NOT EXISTS idx_edge_labels ON edges (label);",

			"CREATE TABLE IF NOT EXISTS vertex_properties ("+
			  "vertex_id text NOT NULL,"+
			  "key text NOT NULL,"+
			  "value TEXT,"+
			  "CONSTRAINT fk_vertex FOREIGN KEY (vertex_id) REFERENCES vertices (id)"+
			    "ON DELETE CASCADE"+
			");",

			"CREATE INDEX IF NOT EXISTS idx_vertex_properties ON vertex_properties (key);",
			"CREATE INDEX IF NOT EXISTS idx_vertex_properties_2 ON vertex_properties (left(value, 200));",
	
			"CREATE TABLE IF NOT EXISTS edge_properties ("+
			  "edge_id text NOT NULL,"+
			  "key text NOT NULL,"+
			  "value TEXT,"+
			  "CONSTRAINT fk_edge FOREIGN KEY (edge_id) REFERENCES edges (id)"+
			    "ON DELETE CASCADE"+ 
			");",

			"CREATE INDEX IF NOT EXISTS idx_edge_properties ON edge_properties (key);",
			"CREATE INDEX IF NOT EXISTS idx_edge_properties_2 ON edge_properties (left(value, 200));"
	};
}
