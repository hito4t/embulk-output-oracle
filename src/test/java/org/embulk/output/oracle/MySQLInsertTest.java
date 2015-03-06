package org.embulk.output.oracle;


public class MySQLInsertTest extends EmbulkPluginTest {
	
	public static void main(String[] args) {
		execute("run", "src/test/resources/test-mysql-insert.yml");
	}
	
}
