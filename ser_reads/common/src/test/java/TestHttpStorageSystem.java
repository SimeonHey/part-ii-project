import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;

public class TestHttpStorageSystem {
    private static final int FREE_PORT = 8002;

    @Test
    public void testDiscover() throws IOException {
        HttpStorageSystem httpStorageSystem =
            new HttpStorageSystem("test", HttpUtils.initHttpServer(FREE_PORT));

        String resp =
            HttpUtils.httpRequestResponse("http://localhost:" + FREE_PORT, "test/discover", "");

        assertEquals(HttpStorageSystem.DISCOVER_PHRASE, resp);
    }

    @Test
    public void testRegisterHandler() throws IOException {
        HttpStorageSystem httpStorageSystem =
            new HttpStorageSystem("test", HttpUtils.initHttpServer(FREE_PORT));

        httpStorageSystem.registerHandler("sayHi", (query) -> ("Hi " + query).getBytes());

        String resp =
            HttpUtils.httpRequestResponse("http://localhost:" + FREE_PORT, "test/sayHi", "Simeon");
        assertEquals("Hi Simeon", resp);
    }
}
