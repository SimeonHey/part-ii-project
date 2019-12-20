import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.util.function.Function;
import java.util.logging.Logger;

public class HttpStorageSystem {
    private static final Logger LOGGER = Logger.getLogger(HttpStorageSystem.class.getName());

    private final HttpServer httpServer;
    private final String storageSystemName;

    public HttpStorageSystem(String storageSystemName, HttpServer httpServer) {
        this.httpServer = httpServer;
        this.storageSystemName = storageSystemName;

        this.registerHandler("discover", this::handleDiscover);
    }

    private byte[] handleDiscover(String query) {
        return new byte[0];
    }

    protected void registerHandler(String endpoint, Function<String, byte[]> handler) {
        this.httpServer.createContext(String.format("/%s/%s", this.storageSystemName, endpoint),
            (httpExchange) -> {
            LOGGER.info(storageSystemName + "handles request at endpoint " + endpoint);
                String query = httpExchange.getRequestURI().getQuery();

                byte[] bytes = handler.apply(query);

                httpExchange.sendResponseHeaders(200, bytes.length);

                OutputStream os = httpExchange.getResponseBody();
                os.write(bytes);
                os.close();

                httpExchange.close();
            });
    }
}
