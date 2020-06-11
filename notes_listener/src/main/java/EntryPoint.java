import io.vavr.Tuple2;

import java.util.List;

public class EntryPoint {
	public static void main(String[] args) {
		String selfAddress = "192.168.1.50";

		PolyglotAPI polyglotAPI = new PolyglotAPI(8000,
			ConstantsNotesApp.KAFKA_ADDRESS_TOPIC,
			selfAddress,
			List.of(new Tuple2<>(NotePlayedEvent.class.getName(),
				List.of("http://localhost:" + ConstantsNotesApp.PERSISTOR_PORT + "/persistor/query"))));

		NotesPersistor.buildAndRunStorageSystem();

		polyglotAPI.handleRequest(new NotePlayedEvent(60, 100));
	}
}
