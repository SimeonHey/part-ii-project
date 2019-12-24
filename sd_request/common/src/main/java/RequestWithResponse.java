public class RequestWithResponse extends BaseRequest{
    protected static final String KEY_RESPONSE_ENDPOINT = "responseEndpoint";

    final String responseEndpoint;

    RequestWithResponse(String responseEndpoint, long uuid) {
        super(uuid);
        this.responseEndpoint = responseEndpoint;
    }

    public String getResponseEndpoint() {
        return responseEndpoint;
    }
}
