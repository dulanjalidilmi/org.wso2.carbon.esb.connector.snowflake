package org.wso2.carbon.connector.utils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.axis2.AxisFault;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.connector.exception.InvalidConfigurationException;
import org.wso2.carbon.connector.pojo.SnowflakesOperationResult;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SnowflakeUtils {

    private static final Log log = LogFactory.getLog(SnowflakeUtils.class);

    public static String getConnectionName(MessageContext messageContext) throws InvalidConfigurationException {
        String connectionName = (String) messageContext.getProperty(Constants.CONNECTION_NAME);
        if (StringUtils.isEmpty(connectionName)) {
            throw new InvalidConfigurationException("Connection name is not set.");
        }
        return connectionName;
    }

    public static void setResultAsPayload(MessageContext msgContext, SnowflakesOperationResult result) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("operation", result.getOperation());
        jsonObject.addProperty("isSuccessful", result.isSuccessful());

        if (result.getMessage() != null && !result.getMessage().isEmpty()) {
            jsonObject.addProperty("message", result.getMessage());
        }

        if (result.getError() != null) {
            setErrorPropertiesToMessageContext(msgContext, result.getError());
            jsonObject.addProperty("error", result.getError().getErrorCode());
            jsonObject.addProperty("code", result.getError().getErrorCode());
            jsonObject.addProperty("errorDetail", result.getError().getErrorDetail());
        }

        JsonUtil.removeJsonPayload(((Axis2MessageContext)msgContext).getAxis2MessageContext());
        ((Axis2MessageContext)msgContext).getAxis2MessageContext().
                removeProperty("NO_ENTITY_BODY");
        try {
            JsonUtil.getNewJsonPayload(((Axis2MessageContext)msgContext).getAxis2MessageContext(),
                    jsonObject.toString(), true, true);
        } catch (AxisFault axisFault) {
            log.error("Error occurred while populating the payload in the message context.", axisFault);
        }
    }

    public static void setResultAsPayload(MessageContext msgContext, JsonArray result) {
        org.apache.axis2.context.MessageContext axisMsgCtx =
                ((Axis2MessageContext) msgContext).getAxis2MessageContext();
        JsonUtil.removeJsonPayload(axisMsgCtx);
        axisMsgCtx.removeProperty("NO_ENTITY_BODY");
        axisMsgCtx.setProperty(Constants.MESSAGE_TYPE, Constants.JSON_CONTENT_TYPE);
        axisMsgCtx.setProperty(Constants.CONTENT_TYPE, Constants.JSON_CONTENT_TYPE);
        try {
            JsonUtil.getNewJsonPayload(((Axis2MessageContext)msgContext).getAxis2MessageContext(),
                    result.toString(), true, true);
        } catch (AxisFault e) {
            log.error("Error occurred while populating the payload in the message context.", e);
        }
    }

    private static void setErrorPropertiesToMessageContext(MessageContext msgContext, Error error) {
        msgContext.setProperty(Constants.PROPERTY_ERROR_CODE, error.getErrorCode());
        msgContext.setProperty(Constants.PROPERTY_ERROR_MESSAGE, error.getErrorDetail());
        Axis2MessageContext axis2smc = (Axis2MessageContext) msgContext;
        org.apache.axis2.context.MessageContext axis2MessageCtx = axis2smc.getAxis2MessageContext();
        axis2MessageCtx.setProperty(Constants.STATUS_CODE, Constants.HTTP_STATUS_500);
    }

    public static String[] getColumnNames(String query) throws InvalidConfigurationException {
        // Using regular expression to match the column names inside the parentheses
        Pattern pattern = Pattern.compile("\\((.*?)\\)");
        Matcher matcher = pattern.matcher(query);
        String[] columnNames = null;

        if (matcher.find()) {
            String columnNamesStr = matcher.group(1);
            columnNames = columnNamesStr.split("\\s*,\\s*");
        }

        if (columnNames == null) {
            throw new InvalidConfigurationException(
                    "Invalid query is provided. Column names are not found in the query.");
        }

        return columnNames;
    }


    public static JsonObject getJsonObject(String payload) {
        Gson gson = new Gson();
        return gson.fromJson(payload, JsonObject.class);
    }

    public static void setError(String operation, MessageContext messageContext, Exception e, Error error) {
        SnowflakesOperationResult result =
                new SnowflakesOperationResult(operation, false, error, e.getMessage());
        setResultAsPayload(messageContext, result);
    }

}
