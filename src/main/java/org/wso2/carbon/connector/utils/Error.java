package org.wso2.carbon.connector.utils;

public enum Error {

    CONNECTION_ERROR("700601", "SNOWFLAKE:CONNECTION_ERROR"),
    INVALID_CONFIGURATION("700602", "SNOWFLAKE:INVALID_CONFIGURATION"),
    OPERATION_ERROR("700603", "SNOWFLAKE:OPERATION_ERROR"),
    RESPONSE_GENERATION("700604", "EMAIL:RESPONSE_GENERATION"),
    INVALID_RESPONSE("700605", "SNOWFLAKE:INVALID_RESPONSE");

    private final String code;
    private final String message;

    /**
     * Create an error code.
     *
     * @param code    error code represented by number
     * @param message error message
     */
    Error(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getErrorCode() {
        return this.code;
    }

    public String getErrorDetail() {
        return this.message;
    }
    
}
