package org.wso2.carbon.connector.pojo;

import org.wso2.carbon.connector.utils.Error;

public class SnowflakesOperationResult {
    private String operation;

    /**
     * Is operation successfully executed
     */
    private boolean isSuccessful = false;

    /**
     * Error code
     */
    private Error error;

    /**
     * Error message
     */
    private String errorMessage;


    public SnowflakesOperationResult(String operation, boolean isSuccessful) {
        this.operation = operation;
        this.isSuccessful = isSuccessful;
    }

    public SnowflakesOperationResult(String operation, boolean isSuccessful, Error error, String errorMessage) {
        this.operation = operation;
        this.isSuccessful = isSuccessful;
        this.error = error;
        this.errorMessage = errorMessage;
    }

    public String getOperation() {
        return operation;
    }

    public boolean isSuccessful() {
        return isSuccessful;
    }
}
