public class NotePlayedEvent extends EventBase {
	private final int noteNumber;
	private final int velocity;

	public NotePlayedEvent(int noteNumber, int velocity) {
		super(false);

		this.noteNumber = noteNumber;
		this.velocity = velocity;
	}

	public int getNoteNumber() {
		return noteNumber;
	}

	public int getVelocity() {
		return velocity;
	}
}
