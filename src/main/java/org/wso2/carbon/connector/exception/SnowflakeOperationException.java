package org.wso2.carbon.connector.exception;

import org.wso2.carbon.connector.core.ConnectException;

public class SnowflakeOperationException extends ConnectException {

    public SnowflakeOperationException(Throwable e) {
        super(e);
    }

    public SnowflakeOperationException(String message) {
        super(message);
    }

    public SnowflakeOperationException(String message, Throwable e) {
        super(e, message);
    }
}
