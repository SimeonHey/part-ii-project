import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class TestHttpStorageSystem {
    private static final int FREE_PORT = 8002;

    @Test
    public void testDiscover() throws IOException {
        new HttpStorageSystem("test", HttpUtils.initHttpServer(FREE_PORT));

        String resp =
            HttpUtils.httpRequestResponse("http://localhost:" + FREE_PORT, "test/discover", "");

        assertEquals(HttpStorageSystem.DISCOVER_PHRASE, resp);
    }

    @Test
    public void testRegisterHandler() throws IOException {
        String phrase = "Simeon'ski!@\\x";

        HttpStorageSystem httpStorageSystem =
            new HttpStorageSystem("test", HttpUtils.initHttpServer(FREE_PORT));

        httpStorageSystem.registerHandler("sayHi", (query) -> ("Hi " + query).getBytes());

        String resp =HttpUtils.httpRequestResponse("http://localhost:" + FREE_PORT, "test/sayHi",
            phrase);
        assertEquals("Hi " + phrase, resp);
    }

    @Test
    public void testHttpRequest() throws IOException {
        String phrase = "Simeon'ski!@\\x";

        HttpStorageSystem httpStorageSystem =
            new HttpStorageSystem("test", HttpUtils.initHttpServer(FREE_PORT));

        List<String> theResponse = new ArrayList<>(1);
        httpStorageSystem.registerHandler("sayHi", (query) -> {
            theResponse.add(query);
            return ("Hi " + query).getBytes();
        });

        HttpUtils.sendHttpRequest("http://localhost:" + FREE_PORT, "test/sayHi", phrase);
        assertEquals(1, theResponse.size());
        assertEquals(theResponse.get(0), phrase);
    }

    @Test
    public void testBigHttpRequest() throws IOException {
        int charsLim = 1_000_000 * 1000;
        String phrase = "a".repeat(charsLim);

        HttpStorageSystem httpStorageSystem =
            new HttpStorageSystem("test", HttpUtils.initHttpServer(FREE_PORT));

        List<String> theResponse = new ArrayList<>(1);
        httpStorageSystem.registerHandler("sayHi", (query) -> {
            theResponse.add(query);
            return ("Hi " + query).getBytes();
        });

        HttpUtils.sendHttpRequest("http://localhost:" + FREE_PORT, "test/sayHi", phrase);
        assertEquals(1, theResponse.size());
        assertEquals(theResponse.get(0), phrase);
    }
}
