import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.logging.Logger;

public class HttpUtils {
    private final static Logger LOGGER = Logger.getLogger(HttpUtils.class.getName());
    private final static int TIMEOUT_MS = 5 * 1000;

    private final static TimeMeasurer httpRequestTimeMeasurer = new TimeMeasurer(Constants.METRIC_REGISTRY,
        "httpTimes");

    static HttpURLConnection sendHttpRequest(String url,
                                             String params) throws IOException {
        var activeTime = httpRequestTimeMeasurer.startTimer();
        LOGGER.info("Sending an HTTP request to " + url + " with body length " + params.length());

        HttpURLConnection httpURLConnection =
            (HttpURLConnection) new URL(url).openConnection();
        httpURLConnection.setRequestMethod("POST");
        httpURLConnection.setConnectTimeout(TIMEOUT_MS);

        httpURLConnection.setDoInput(true);
        httpURLConnection.setDoOutput(true);

        httpURLConnection.getOutputStream().write(params.getBytes());
        httpURLConnection.getOutputStream().close();

        httpURLConnection.getInputStream();

        httpRequestTimeMeasurer.stopTimerAndPublish(activeTime);
        LOGGER.info("Sending to " + url + " done!");
        return httpURLConnection;
    }

    static HttpURLConnection sendHttpRequest(String base,
                                             String endpoint,
                                             String params) throws IOException {
        String url = String.format("%s/%s", base, endpoint);
        return sendHttpRequest(url, params);
    }

    static String httpRequestResponse(String base,
                                      String endpoint,
                                      String params) throws IOException {
        HttpURLConnection conn = sendHttpRequest(base, endpoint, params);
        return new String(conn.getInputStream().readAllBytes());
    }

    static String httpRequestResponse(String full, String params) throws IOException {
        var activeTime = httpRequestTimeMeasurer.startTimer();

        HttpURLConnection conn = sendHttpRequest(full, params);
        String res = new String(conn.getInputStream().readAllBytes());

        httpRequestTimeMeasurer.stopTimerAndPublish(activeTime);
        return res;
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
