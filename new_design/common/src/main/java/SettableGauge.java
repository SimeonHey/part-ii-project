import com.codahale.metrics.Gauge;

import java.util.concurrent.atomic.AtomicReference;

public class SettableGauge<T> implements Gauge<T> {
    private AtomicReference<T> object = new AtomicReference<>();

    public void setValue(T value) {
        object.set(value);
    }

    @Override
    public T getValue() {
        return object.get();
    }
}
