public class PsqlWrapper {
    public void postMessage(PostMessageRequest postMessageRequest, Long uuid) {
        System.out.println("PSQL posts message " + postMessageRequest + " with uuid " + uuid);
    }

    public String getMessageDetails(MessageDetailsRequest messageDetailsRequest) {
        return "Psql has to get details for message " + messageDetailsRequest.getUuid() + " but too lazy.";
    }
}
// TODO: Junit