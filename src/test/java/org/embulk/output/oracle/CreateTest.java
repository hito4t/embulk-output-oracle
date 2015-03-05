/*
 * $Id: typical.epf 2627 2010-03-18 01:40:13Z tiba $
 */
package org.embulk.output.oracle;

import org.embulk.cli.Main;

public class CreateTest {
	
	public static void main(String[] args) {
		Main.main(new String[]{"run", "src/test/resources/test-create.yml"});
	}
	
}
