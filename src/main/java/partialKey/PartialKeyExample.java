package partialKey;

import swaydb.java.*;
import swaydb.java.data.slice.ByteSlice;
import swaydb.java.memory.MapConfig;
import swaydb.java.memory.SetConfig;
import swaydb.java.serializers.Serializer;

import java.io.*;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static swaydb.java.serializers.Default.intSerializer;

/**
 * A Partial Key is a Key that has partial ordering applied to it
 * and can be read as full key from the a Set or a Map by submitting
 * the partial key.
 */
class PartialKeyExample {

  private static class Key implements Serializable {
    int id;
    String userName;


    public Key(int id, String userName) {
      this.id = id;
      this.userName = userName;
    }

    @Override
    public String toString() {
      return "Key{" +
        "id=" + id +
        ", userName='" + userName + '\'' +
        '}';
    }
  }

  //VERY SLOW serializer using ObjectOutputStream. Use a different serialisation library instead.
  static Serializer<Key> serializer =
    new Serializer<Key>() {
      @Override
      public byte[] write(Key data) {
        try {
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream(bos);
          oos.writeObject(data);
          oos.flush();
          return bos.toByteArray();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Key read(ByteSlice slice) {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(slice.toByteBufferWrap().array(), slice.fromOffset(), slice.size());
        try {
          ObjectInputStream oos = new ObjectInputStream(byteArrayInputStream);
          return (Key) oos.readObject();
        } catch (IOException | ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    };

  //partial key comparator
  static KeyComparator<Key> comparator =
    new KeyComparator<Key>() {
      @Override
      public int compare(Key o1, Key o2) {
        return Integer.compare(o1.id, o2.id);
      }

      @Override
      public Key comparableKey(Key data) {
        //since above compare is done only on id set the value of string to a static value.
        return new Key(data.id, "");
      }
    };

  public static void main(String[] args) {
    //create a memory database with functions enabled.
    SetConfig.Config<Key, Void> setConfig = SetConfig.withoutFunctions(serializer);
    setConfig.setComparator(IO.rightNeverException(comparator));
    Set<Key, Void> set = setConfig.init();

    set.add(new Key(1, "one"));

    //partial read where userName is empty
    Optional<Key> one = set.get(new Key(1, ""));
    System.out.println(one); //prints the full key with userName set
  }
}
