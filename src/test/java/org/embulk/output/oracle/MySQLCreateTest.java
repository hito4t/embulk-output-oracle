package org.embulk.output.oracle;

import org.embulk.cli.Main;

public class MySQLCreateTest {
	
	public static void main(String[] args) {
		Main.main(new String[]{"run", "src/test/resources/test-mysql-create.yml"});
	}
	
}
