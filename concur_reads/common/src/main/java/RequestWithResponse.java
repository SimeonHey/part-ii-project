public class RequestWithResponse extends BaseRequest{
    protected static final String KEY_RESPONSE_ENDPOINT = "responseEndpoint";

    final String responseEndpoint;

    @Override
    public String toString() {
        return "RequestWithResponse{" +
            "responseEndpoint='" + responseEndpoint + '\'' +
            "SUPER: {" + super.toString() + "}" +
            '}';
    }

    RequestWithResponse(String responseEndpoint, long uuid) {
        super(uuid);
        this.responseEndpoint = responseEndpoint;
    }

    public String getResponseEndpoint() {
        return responseEndpoint;
    }
}
