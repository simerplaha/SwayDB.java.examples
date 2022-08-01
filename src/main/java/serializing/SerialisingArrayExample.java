package serializing;

import swaydb.data.slice.Slice;
import swaydb.data.slice.SliceReader;
import swaydb.data.util.ByteOps;
import swaydb.java.Map;
import swaydb.java.memory.MemoryMap;
import swaydb.java.serializers.Serializer;

import java.util.Arrays;

import static swaydb.java.serializers.Default.intSerializer;

/**
 * <a href="https://github.com/simerplaha/SwayDB/discussions/361">Question asked #361</a>
 */
class SerialisingArrayExample {

  public static void main(String[] args) {
    Serializer<long[]> longArraySerializer = new Serializer<long[]>() {
      @Override
      public Slice<Byte> write(long[] data) {
        int requiredHeaderBytes = Integer.BYTES; //for storing the length of array
        int requiredDataBytes = data.length * Long.BYTES; //for storing actual data
        int totalBytesRequired = requiredHeaderBytes + requiredDataBytes; //total count
        Slice<Byte> slice = Slice.ofBytesJava(totalBytesRequired); //create a slice instance
        slice.addInt(data.length, ByteOps.Java()); //write the number of longs in the array
        for (Long longVal : data) { //write the long[] values
          slice.addLong(longVal, ByteOps.Java());
        }
        return slice; //return the slice
      }

      @Override
      public long[] read(Slice<Byte> slice) {
        SliceReader<Byte> reader = slice.createReader(ByteOps.Java()); //create reader from the slice
        int longCount = reader.readInt(); //read header: number of longs
        long[] longs = new long[longCount]; //create the long array to add to
        for (int i = 0; i < longCount; i++) {
          longs[i] = reader.readLong(); //read the long values and set it in the array
        }
        return longs; //return the long
      }
    };

    Map<Integer, long[], Void> map = MemoryMap.functionsOff(intSerializer(), longArraySerializer).get();

    //add some data
    map.put(1, new long[]{1L, 2L, 3L, 4L});
    map.put(2, new long[]{5L, 6L, 7L, 8L});
    map.put(3, new long[]{9L, 10L, 11L, 12L});

    //print out the key-values
    map.forEach(keyValue -> {
      System.out.println("Key: " + keyValue.key());
      System.out.println("Value: " + Arrays.toString(keyValue.value()));
    });
  }
}

