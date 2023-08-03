package org.wso2.carbon.connector.operations;

import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.connector.connection.SnowflakeConnection;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.connector.utils.Constants;

public class SnowflakeConfig extends AbstractConnector implements ManagedLifecycle {

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        String accountIdentifier =
                (String) ConnectorUtils.lookupTemplateParamater(messageContext, Constants.ACCOUNT_IDENTIFIER);
        String user = (String) ConnectorUtils.lookupTemplateParamater(messageContext, Constants.USER);
        String password = (String) ConnectorUtils.lookupTemplateParamater(messageContext, Constants.PASSWORD);

        // todo validation of data

        createConnection(messageContext);

    }

    private void createConnection(MessageContext messageContext) {
        // todo lets add this to ConnectionConfiguration class
        String connectorName = Constants.CONNECTOR_NAME;
        String connectionName = (String) ConnectorUtils.
                lookupTemplateParamater(messageContext, Constants.CONNECTION_NAME);
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        if (!handler.checkIfConnectionExists(connectorName, connectionName)) {
            SnowflakeConnection connection = new SnowflakeConnection(messageContext);
            handler.createConnection(connectorName, connectionName, connection);
        } else {
            log.debug("Connection already exists with name : " + connectionName);
        }
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
