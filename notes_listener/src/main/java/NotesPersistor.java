import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.logging.Logger;

public class NotesPersistor extends SnapshottedDatabase<Void> {
	private final static Logger LOGGER = Logger.getLogger(NotesPersistor.class.getName());

	private final Path writePath;

	protected NotesPersistor(Path writePath) {
		super(0);
		this.writePath = writePath;
	}

	void notePlayed(NotePlayedEvent notePlayedEvent) {
		String toAppend = String.format("%d, %d\n", notePlayedEvent.getNoteNumber(), notePlayedEvent.getVelocity());
		try {
			Files.write(writePath, toAppend.getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
		} catch (IOException e) {
			LOGGER.warning("Error when trying to persist last note to path: " + e);
			throw new RuntimeException(e);
		}
	}

	@Override
	Void getMainDataView() {
		return null;
	}

	@Override
	Void freshConcurrentSnapshot() {
		return null;
	}

	@Override
	Void refreshSnapshot(Void bareSnapshot) {
		return null;
	}

	@Override
	public void close() throws Exception {

	}

	static StorageSystem<Void> buildAndRunStorageSystem() {
		NotesPersistor wrapper = new NotesPersistor(Paths.get("notes_persist_file.txt"));

		return new StorageSystemBuilder<Void>("persistor", wrapper, ConstantsNotesApp.PERSISTOR_PORT,
			ConstantsNotesApp.KAFKA_ADDRESS_TOPIC)
			.registerAction(new ActionBase<Void>(NotePlayedEvent.class, false) {
				@Override
				Response handleEvent(EventBase request, StorageSystem<Void> self, Void snapshot) {
					LOGGER.info("In the body!!!");
					wrapper.notePlayed((NotePlayedEvent) request);
					return Response.CONFIRMATION;
				}
			})
			.buildAndRun();
	}
}
