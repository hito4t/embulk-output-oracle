package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.setter.ColumnSetterFactory;
import org.embulk.output.oracle.OracleBatchInsert;
import org.embulk.output.oracle.OracleOutputConnector;
import org.embulk.output.oracle.setter.OracleColumnSetterFactory;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.TimestampFormatter;

import com.google.common.base.Optional;


public class OracleOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface OraclePluginTask
            extends PluginTask
    {
        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("1521")
        public int getPort();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("database")
        public String getDatabase();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return OraclePluginTask.class;
    }

    @Override
    protected OracleOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        OraclePluginTask oracleTask = (OraclePluginTask) task;
        
        if (oracleTask.getDriverPath().isPresent()) {
        	// TODO:スーパークラスを利用
            // TODO:後でなんとかする。今は直JARをクラスパスに追加
            // PluginClassLoader loader = (PluginClassLoader) getClass().getClassLoader();
            //loader.addPath(Paths.get(oracleTask.getDriverPath().get()));
        }

        String url = String.format("jdbc:oracle:thin:@%s:%d:%s",
                oracleTask.getHost(), oracleTask.getPort(), oracleTask.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", oracleTask.getUser());
        props.setProperty("password", oracleTask.getPassword());

        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("useCompression", "true");

        props.setProperty("connectTimeout", "300000"); // milliseconds
        props.setProperty("socketTimeout", "1800000"); // smillieconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        // TODO
        //switch t.getSssl() {
        //when "disable":
        //    break;
        //when "enable":
        //    props.setProperty("useSSL", "true");
        //    props.setProperty("requireSSL", "false");
        //    props.setProperty("verifyServerCertificate", "false");
        //    break;
        //when "verify":
        //    props.setProperty("useSSL", "true");
        //    props.setProperty("requireSSL", "true");
        //    props.setProperty("verifyServerCertificate", "true");
        //    break;
        //}

        if (!retryableMetadataOperation) {
            // non-retryable batch operation uses longer timeout
            props.setProperty("connectTimeout",  "300000");  // milliseconds
            props.setProperty("socketTimeout", "2700000");   // milliseconds
        }

        props.putAll(oracleTask.getOptions());

        return new OracleOutputConnector(url, props);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task) throws IOException, SQLException
    {
        return new OracleBatchInsert(getConnector(task, true));
    }
    
    @Override
    protected ColumnSetterFactory newColumnSetterFactory(BatchInsert batch, PageReader pageReader,
    		TimestampFormatter timestampFormatter) {
    	return new OracleColumnSetterFactory(batch, pageReader, timestampFormatter);
    }
    
}
