package org.wso2.carbon.connector.utils;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPBody;
import org.apache.axis2.AxisFault;
import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.wso2.carbon.connector.pojo.SnowflakesOperationResult;

public class SnowflakeUtils {
    public static String getConnectionName(MessageContext messageContext) {
        String connectionName = (String) messageContext.getProperty(Constants.CONNECTION_NAME);
        if (connectionName == null) {
            // todo handle this
            throw new RuntimeException("Connection name is not set.");
        }
        return connectionName;
    }

    public static void setResultAsPayload(MessageContext msgContext,
                                          SnowflakesOperationResult snowflakesOperationResult) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("operation", snowflakesOperationResult.getOperation());
            jsonObject.put("isSuccessful", snowflakesOperationResult.isSuccessful());
            if (snowflakesOperationResult.getMessage() != null || !snowflakesOperationResult.getMessage().isEmpty()) {
                jsonObject.put("message", snowflakesOperationResult.getMessage());
            }
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }

        JsonUtil.removeJsonPayload(((Axis2MessageContext)msgContext).getAxis2MessageContext());
        ((Axis2MessageContext)msgContext).getAxis2MessageContext().
                removeProperty("NO_ENTITY_BODY");
        try {
            JsonUtil.getNewJsonPayload(((Axis2MessageContext)msgContext).getAxis2MessageContext(), jsonObject.toString(), true, true);
        } catch (AxisFault e) {
            throw new RuntimeException(e);
        }
    }
}
