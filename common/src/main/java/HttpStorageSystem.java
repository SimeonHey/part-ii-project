import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;

public class HttpStorageSystem {
    public HttpStorageSystem(String storageSystemName, HttpServer httpServer) {
        httpServer.createContext(String.format("/%s/discover", storageSystemName), this::handleDiscover);
    }

    private void handleDiscover(HttpExchange httpExchange) throws IOException {
        httpExchange.sendResponseHeaders(200, 0);
        httpExchange.close();
    }
}
