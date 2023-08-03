package org.wso2.carbon.connector.operations;

import org.apache.synapse.MessageContext;
import org.wso2.carbon.connector.connection.SnowflakeConnection;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.connection.ConnectionHandler;
import org.wso2.carbon.connector.utils.Constants;
import org.wso2.carbon.connector.utils.SnowflakeUtils;

public class Execute extends AbstractConnector {
    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        System.out.println("Executing....");
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        String connectionName = SnowflakeUtils.getConnectionName(messageContext);
        SnowflakeConnection snowflakeConnection =
                (SnowflakeConnection) handler.getConnection(Constants.CONNECTOR_NAME, connectionName);
        execute(messageContext, snowflakeConnection);
    }

    private void execute(MessageContext messageContext, SnowflakeConnection snowflakeConnection) {
        String query = (String) getParameter(messageContext, Constants.EXECUTE_QUERY);
        Object payload = getParameter(messageContext, Constants.PAYLOAD);

        if(query == null || query.isEmpty()) {
            // todo throw exception
            log.error("Query is empty");
            return;
        }
        if(payload == null) {
            // todo throw exception
            log.error("Payload is empty");
            return;
        }

        log.info("executing...");

    }
}
