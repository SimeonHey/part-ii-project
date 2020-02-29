import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.util.function.Function;
import java.util.logging.Logger;

public class HttpStorageSystem implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(HttpStorageSystem.class.getName());
    public static final String DISCOVER_PHRASE = "I'm here";

    private final HttpServer httpServer;
    private final String storageSystemName;
    private final int port;

    public HttpStorageSystem(String storageSystemName, HttpServer httpServer) {
        LOGGER.info("Initializing HTTP system " + storageSystemName + " on address " + httpServer.getAddress());
        this.httpServer = httpServer;
        this.storageSystemName = storageSystemName;
        this.port = httpServer.getAddress().getPort();

        this.registerHandler("discover", this::handleDiscover);
    }

    private byte[] handleDiscover(String query) {
        return DISCOVER_PHRASE.getBytes();
    }

    protected void registerHandler(String endpoint, Function<String, byte[]> handler) {
        String fullEndpoint = String.format("/%s/%s", this.storageSystemName, endpoint);
        LOGGER.info("HTTP system handler initialization on " + fullEndpoint +
            " (port " + port + ")");

        this.httpServer.createContext(fullEndpoint,
            (httpExchange) -> {
                LOGGER.info(storageSystemName + " handles request at endpoint " + endpoint);

                String bodyQuery = new String(httpExchange.getRequestBody().readAllBytes());
                LOGGER.info("Post data is " + bodyQuery);

                byte[] bytes = handler.apply(bodyQuery);

                httpExchange.sendResponseHeaders(200, bytes.length);

                OutputStream os = httpExchange.getResponseBody();
                os.write(bytes);
                os.close();

                httpExchange.close();
            });
    }

    public int getPort() {
        return port;
    }

    public String getStorageSystemName() {
        return storageSystemName;
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("Stopping the HTTP server");
        httpServer.stop(0);
    }
}
