package swaydb.java;

import scala.Tuple2;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.Seq;
import scala.runtime.AbstractFunction1;
import swaydb.data.IO;

import java.util.AbstractMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * The StreamImpl of data.
 *
 * @param <K> the type of the key element
 * @param <V> the type of the value element
 */
public class StreamImpl<K, V> implements swaydb.java.Stream<K, V> {
    private final swaydb.Stream<Tuple2<K, V>, IO> streamObject;
    private final IO.Success success;

    /**
     * Constructs the Stream object.
     * @param streamObject the streamObject
     */
    public StreamImpl(final swaydb.Stream<Tuple2<K, V>, IO> streamObject) {
        this.streamObject = streamObject;
        this.success = null;
    }

    /**
     * Constructs the Stream object.
     * @param success the success
     */
    public StreamImpl(final IO.Success success) {
        this.streamObject = null;
        this.success = success;
    }

    /**
     * Starts the map function for this map.
     * @param function the function
     *
     * @return the stream object for this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public swaydb.java.Stream<K, V> map(UnaryOperator<Map.Entry<K, V>> function) {
        return new StreamImpl<K, V>(streamObject.map(new AbstractFunction1() {
            public Object apply(Object tuple2) {
                java.util.Map.Entry<K, V> result = function.apply(
                        new AbstractMap.SimpleEntry<>((K) ((Tuple2) tuple2)._1(), (V) ((Tuple2) tuple2)._2()));
                return IO.Success$.MODULE$.apply(Tuple2.apply(result.getKey(), result.getValue()));
            }
        }));
    }

    /**
     * Starts the materialize function for this map.
     *
     * @return the stream object for this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public Stream<K, V> materialize() {
        return new StreamImpl<>((IO.Success) streamObject.materialize());
    }

    /**
     * Starts the foreach function for this map.
     * @param consumer the consumer
     *
     * @return the stream object for this map
     */
    @SuppressWarnings("unchecked")
    @Override
    public Stream<K, V> foreach(Consumer<Map.Entry<K, V>> consumer) {
        success.foreach(new AbstractFunction1<Object, Object>() {
            @Override
            public Object apply(Object t1) {
                Seq entries =  ((ListBuffer) t1).seq();
                for (int index = 0; index < entries.size(); index += 1) {
                    IO.Success<Tuple2<K, V>> tuple2 = (IO.Success<Tuple2<K, V>>) entries.apply(index);
                    consumer.accept(new AbstractMap.SimpleEntry<>(tuple2.get()._1(), tuple2.get()._2()));
                }
                return null;
            }
        });
        return this;
    }
}
