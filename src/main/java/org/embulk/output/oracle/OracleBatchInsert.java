package org.embulk.output.oracle;

import java.io.IOException;
import java.sql.Types;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.embulk.output.jdbc.StandardBatchInsert;

public class OracleBatchInsert
        extends StandardBatchInsert
{
    public OracleBatchInsert(OracleOutputConnector connector) throws IOException, SQLException
    {
        super(connector);
    }

    @Override
    public void setFloat(float v) throws IOException, SQLException
    {
        if (Float.isNaN(v) || Float.isInfinite(v)) {
            setNull(Types.REAL);  // TODO get through argument
        } else {
            super.setFloat(v);
        }
    }

    @Override
    public void setDouble(double v) throws IOException, SQLException
    {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            setNull(Types.DOUBLE);  // TODO get through argument
        } else {
            super.setDouble(v);
        }
    }
}
