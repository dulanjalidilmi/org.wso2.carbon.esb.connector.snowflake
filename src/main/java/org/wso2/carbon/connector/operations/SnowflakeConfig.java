package org.wso2.carbon.connector.operations;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.connector.connection.SnowflakeConnection;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.connector.exception.InvalidConfigurationException;
import org.wso2.carbon.connector.pojo.ConnectionConfiguration;
import org.wso2.carbon.connector.utils.Constants;
import org.wso2.carbon.connector.utils.Error;
import org.wso2.carbon.connector.utils.SnowflakeUtils;

import java.sql.SQLException;

import static java.lang.String.format;

public class SnowflakeConfig extends AbstractConnector implements ManagedLifecycle {

    private static final Log log = LogFactory.getLog(SnowflakeConfig.class);

    private static final String OPERATION_NAME = "init";

    @Override
    public void connect(MessageContext messageContext) {

        String connectorName = Constants.CONNECTOR_NAME;
        String connectionName = (String) ConnectorUtils.
                lookupTemplateParamater(messageContext, Constants.CONNECTION_NAME);

        try {
            ConnectionConfiguration configuration = getConnectionConfigFromContext(messageContext);
            ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

            if (!handler.checkIfConnectionExists(connectorName, connectionName)) {
                // todo later remove this
                log.info(format("Connection does not exist for %s connector with connection name: %s. " +
                        "Hence a new connection will be created.", connectorName, connectionName));
                if (log.isDebugEnabled()) {
                    log.debug(format("Connection does not exist for %s connector with connection name: %s. " +
                            "Hence a new connection will be created.", connectorName, connectionName));
                }

                SnowflakeConnection snowflakeConnection = new SnowflakeConnection(configuration);
                handler.createConnection(Constants.CONNECTOR_NAME, connectionName, snowflakeConnection);
            } else {
                // todo later remove this
                log.info(format("Connection exists for %s connector with connection name: %s.", connectorName,
                        connectionName));
                if (log.isDebugEnabled()) {
                    log.debug(format("Connection exists for %s connector with connection name: %s.", connectorName,
                            connectionName));
                }
            }
        } catch (InvalidConfigurationException e) {
            String errorDetail = "[" + connectionName + "] Failed to initiate snowflake connector configuration.";
            handleError(messageContext, e, Error.INVALID_CONFIGURATION, errorDetail);
        } catch (SQLException e) {
            String errorDetail = "[" + connectionName + "] Failed to initiate snowflake connection.";
            handleError(messageContext, e, Error.OPERATION_ERROR, errorDetail);
        }
    }

    private void handleError(MessageContext messageContext, Exception e, Error error, String errorDetail) {
        SnowflakeUtils.setError(OPERATION_NAME, messageContext, e, error);
        handleException(errorDetail, e, messageContext);
    }

    private ConnectionConfiguration getConnectionConfigFromContext(MessageContext messageContext)
            throws InvalidConfigurationException {
        String accountIdentifier =
                (String) ConnectorUtils.lookupTemplateParamater(messageContext, Constants.ACCOUNT_IDENTIFIER);
        String user = (String) ConnectorUtils.lookupTemplateParamater(messageContext, Constants.USER);
        String password = (String) ConnectorUtils.lookupTemplateParamater(messageContext, Constants.PASSWORD);

        ConnectionConfiguration connectionConfiguration = new ConnectionConfiguration();
        connectionConfiguration.setAccountIdentifier(accountIdentifier);
        connectionConfiguration.setUser(user);
        connectionConfiguration.setPassword(password);
        return connectionConfiguration;
    }

    @Override
    public void init(SynapseEnvironment synapseEnvironment) {
        // Nothing to do when initiating the connector
    }

    @Override
    public void destroy() {
        ConnectionHandler.getConnectionHandler().shutdownConnections(Constants.CONNECTOR_NAME);
    }

}
