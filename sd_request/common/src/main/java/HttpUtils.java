import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.logging.Logger;

public class HttpUtils {
    private final static Logger LOGGER = Logger.getLogger(HttpUtils.class.getName());
    private final static int TIMEOUT_MS = 5 * 1000;

    static HttpURLConnection sendHttpRequest(String base,
                                             String endpoint,
                                             String params) throws IOException {
        String url = String.format("%s/%s", base, endpoint);

        LOGGER.info("Sending an HTTP request to " + url + " with body " + params);

        HttpURLConnection httpURLConnection =
            (HttpURLConnection) new URL(url).openConnection();
        httpURLConnection.setRequestMethod("POST");
        httpURLConnection.setConnectTimeout(TIMEOUT_MS);

        httpURLConnection.setDoInput(true);
        httpURLConnection.setDoOutput(true);

        httpURLConnection.getOutputStream().write(params.getBytes());

        return httpURLConnection;
    }

    static String httpRequestResponse(String base,
                                      String endpoint,
                                      String params) throws IOException {
        HttpURLConnection conn = sendHttpRequest(base, endpoint, params);
        return new String(conn.getInputStream().readAllBytes());
    }

    static void discoverEndpoint(String endpoint) throws InterruptedException {
        while (true) {
            String url = String.format("%s/discover", endpoint);
            try {
                HttpURLConnection conn =
                    sendHttpRequest(endpoint, "discover", "");
                if (conn.getResponseCode() != 200) {
                    throw new IOException();
                }
                break;
            } catch (IOException e) {
                LOGGER.warning("Couldn't connect to " + url + ". Retrying...");
                Thread.sleep(1000);
            }
        }
    }

    static HttpServer initHttpServer(int onPort) throws IOException {
        HttpServer httpServer =
            HttpServer.create(new InetSocketAddress(onPort), 0);
        httpServer.setExecutor(null);
        httpServer.start();

        return httpServer;
    }
}
