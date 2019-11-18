public class PsqlWrapper {
    public void postMessage(PostMessageRequest postMessageRequest) {
        System.out.println("PSQL posts message" + postMessageRequest);
    }

    public String getMessageDetails(MessageDetailsRequest messageDetailsRequest) {
        return "Psql has to get details for message " + messageDetailsRequest.getUuid() + " but too lazy.";
    }
}
