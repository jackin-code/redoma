package com.jackin.common;

import com.jackin.Schedule.JobEntry;
import com.jackin.common.EmbeddedConstant.DatabaseType;
import com.jackin.config.OracleConfiguration;
import com.jackin.entity.EmbeddedConfig;
import com.jackin.entity.Job;
import com.jackin.repositories.ClientMongoOpertor;
import io.debezium.embedded.EmbeddedEngine;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.sql.Statement;
import java.text.MessageFormat;

/**
 * @author huangjq
 * @ClassName: EmbeddedUtil
 * @Description: TODO
 * @date 2017/6/16 11:17
 * @since 1.0
 */
public class EmbeddedUtil {

    private static Logger logger = LogManager.getLogger();

    public static Runnable prepare(String storePath, Job job, ClientMongoOpertor clientMongoOpertor) throws SQLException {
        Runnable runnable = null;
        EmbeddedConfig config = job.getConfig();
        String type = config.getDatabase_type();
        DatabaseType databaseType = DatabaseType.fromString(type);
        if (databaseType != null) {
            switch (databaseType) {
                case MYSQL:
                    runnable = createMySqlEngine(storePath, job, clientMongoOpertor);
                    break;
                case ORACLE:
                    runnable = createOracleEngine(storePath, job, clientMongoOpertor);
                    break;
                default:
                    logger.warn(MessageFormat.format("not support database type:[{}]", type));
            }
        }

        return runnable;
    }

    private static Runnable createOracleEngine(String storePath, Job job, ClientMongoOpertor clientMongoOpertor) throws SQLException {
        EmbeddedConfig jobConfig = job.getConfig();
        String host = jobConfig.getDatabase_host();
        Integer port = jobConfig.getDatabase_port();
        String username = jobConfig.getDatabase_username();
        String password = jobConfig.getDatabase_password();
        String database = jobConfig.getDatabase_name();
        final Connection connection = OracleUtil.createConnection(username, password, host, port, database);
        final Statement statement = connection.createStatement();

        final OracleConfiguration config = OracleUtil.createConfig(job, storePath);
        Runnable runnable = () -> {
            ResultSet resultSet = null;
            while (true) {
                try {
                    // add redo log to logminor
                    CallableStatement callableStatement = connection.prepareCall(config.getAddLogminorSql());
                    callableStatement.execute();

                    // start analyze log
                    callableStatement = connection.prepareCall(config.getStartLogminorSql());
                    callableStatement.execute();

                    // read ayalyze report
                    resultSet = statement.executeQuery(config.getLogConetentSql());

                    while (resultSet.next()) {
                        String lastScn = (String) resultSet.getObject(1);
                        if( lastScn.equals(config.getLastSCN()) ){
                            continue;
                        }

                        String operation = resultSet.getObject(2)+"";
                        if( "DDL".equalsIgnoreCase(operation) ){
                            createDictionary(connection, config.getBuildDictionarySql());
                        }
                        String sql = (String) resultSet.getObject(5);
                        net.sf.jsqlparser.statement.Statement stmt = CCJSqlParserUtil.parse(sql);
                        parseOracleStatement(stmt);

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        return runnable;
    }

    private static Runnable createMySqlEngine(String storePath, Job job, ClientMongoOpertor clientMongoOpertor){
        JobEntry jobEntry = new JobEntry(job, clientMongoOpertor);
        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(MySqlUtil.createConfig(job, storePath))
                .notifying(jobEntry::insertIntoMongo)
                .build();
        return engine;
    }

    public static void createDictionary(Connection sourceConn, String buildDictSql) throws Exception{
        CallableStatement callableStatement = sourceConn.prepareCall(buildDictSql);
        callableStatement.execute();
    }

    private static String parseOracleStatement(net.sf.jsqlparser.statement.Statement statement) {

        if (statement != null) {

            if (statement instanceof Insert) {


            } else if (statement instanceof Update) {

            } else if (statement instanceof Delete) {

            } else if (statement instanceof Truncate) {

            }

        }

        return null;
    }
}
