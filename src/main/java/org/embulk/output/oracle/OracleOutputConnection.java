package org.embulk.output.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import org.embulk.spi.Exec;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcColumn;

public class OracleOutputConnection
        extends JdbcOutputConnection
{
    public OracleOutputConnection(Connection connection, boolean autoCommit)
            throws SQLException
    {
        super(connection, null);
        connection.setAutoCommit(autoCommit);
    }

    @Override
    protected String convertTypeName(String typeName)
    {
        switch(typeName) {
        case "CLOB":
            return "TEXT";
        default:
            return typeName;
        }
    }
}
