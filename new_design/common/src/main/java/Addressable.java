import java.io.Serializable;

public class Addressable implements Serializable {
    private final String internetAddress;
    private Long channelID = null;

    public Addressable(Addressable addressable) {
        this.internetAddress = addressable.getInternetAddress();
        this.channelID = addressable.getChannelID();
    }

    public Addressable(String internetAddress, Long channelID) {
        this.channelID = channelID;
        this.internetAddress = internetAddress;
    }

    public Addressable(String internetAddress) {
        this.internetAddress = internetAddress;
    }

    public Long getChannelID() {
        return channelID;
    }

    public String getInternetAddress() {
        return internetAddress;
    }

    public void setChannelID(Long channelID) {
        this.channelID = channelID;
    }

    @Override
    public String toString() {
        return "Address{" +
            "channelID=" + channelID +
            ", internetAddress='" + internetAddress + '\'' +
            '}';
    }
}
