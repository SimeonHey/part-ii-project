public class Response {
    public static final Response CONFIRMATION = new Response();

    private final boolean isResponse;
    private final Object responseToSendBack;

    private Response() {
        isResponse = false;
        responseToSendBack = null;
    }

    public Response(Object responseToSendBack) {
        this.isResponse = true;
        this.responseToSendBack = responseToSendBack;
    }

    public boolean isResponse() {
        return isResponse;
    }

    public Object getResponseToSendBack() {
        return responseToSendBack;
    }

    @Override
    public String toString() {
        return "Response{" +
            "isResponse=" + isResponse +
            ", responseToSendBack=" + responseToSendBack +
            '}';
    }
}
