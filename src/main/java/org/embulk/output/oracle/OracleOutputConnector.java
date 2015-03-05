package org.embulk.output.oracle;

import java.util.Properties;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.SQLException;

import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.JdbcOutputConnection;

public class OracleOutputConnector
        implements JdbcOutputConnector
{
    private final Driver driver;
    private final String url;
    private final Properties properties;

    public OracleOutputConnector(String url, Properties properties)
    {
        try {
            //this.driver = new com.mysql.jdbc.Driver();  // new com.mysql.jdbc.Driver throws SQLException
        	Class<? extends Driver> driverClass = (Class<? extends Driver>)Class.forName("oracle.jdbc.OracleDriver");
        	this.driver = driverClass.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        this.url = url;
        this.properties = properties;
    }

    // TODO:スーパークラスを利用するようにしたい
    @Override
    public OracleOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = driver.connect(url, properties);
        if (c == null) {
        	// driver.connect returns null when url is "jdbc:mysql://...".
        	throw new SQLException("Invalid url : " + url);
        }
        
        try {
            OracleOutputConnection con = new OracleOutputConnection(c, autoCommit);
            c = null;
            return con;
            
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
