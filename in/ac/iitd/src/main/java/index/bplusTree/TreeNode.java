package index.bplusTree;
import java.nio.ByteBuffer;
import java.util.Date;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }

    default public int compare(T key1, T key2) {
        if (key1 instanceof Comparable && key2 instanceof Comparable) {
            Comparable<T> comparableKey1 = (Comparable<T>) key1;
            return comparableKey1.compareTo(key2);
        }
        // Default comparison result if Comparable interface is not implemented
        return key1.toString().compareTo(key2.toString());
    }
    
    default public byte[] convertTToBytes(T key) {
        if (key instanceof String) {
            return ((String) key).getBytes();
        } else if (key instanceof Boolean) {
            return new byte[] { (byte) (((Boolean) key) ? 1 : 0) };
        } else if (key instanceof Byte) {
            return new byte[] { (Byte) key };
        } else if (key instanceof Short) {
            ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
            buffer.putShort((Short) key);
            return buffer.array();
        } else if (key instanceof Integer) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt((Integer) key);
            return buffer.array();
        } else if (key instanceof Long) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong((Long) key);
            return buffer.array();
        } else if (key instanceof Float) {
            ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES);
            buffer.putFloat((Float) key);
            return buffer.array();
        } else if (key instanceof Double) {
            ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
            buffer.putDouble((Double) key);
            return buffer.array();
        } else if (key instanceof Date) {
            return ByteBuffer.allocate(Long.BYTES).putLong(((Date) key).getTime()).array();
        } else {
            // Handle unsupported types or throw an exception
            throw new IllegalArgumentException("Unsupported type: " + key.getClass());
        }
    }
    
    // Might be useful for you
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass){

        /* write code here */
        if (typeClass.equals(Integer.class)) {
            return (T) Integer.valueOf(ByteBuffer.wrap(bytes).getInt());
        } else if (typeClass.equals(String.class)) {
            return (T) new String(bytes);
        } else if (typeClass.equals(Double.class)) {
            return (T) Double.valueOf(ByteBuffer.wrap(bytes).getDouble());
        } else if (typeClass.equals(Date.class)) {
            long milliseconds = ByteBuffer.wrap(bytes).getLong();
            return (T) new Date(milliseconds);
        } else if (typeClass.equals(Boolean.class)) {
            return (T) Boolean.valueOf(bytes[0] != 0);
        } else if (typeClass.equals(Byte.class)) {
            return (T) Byte.valueOf(bytes[0]);
        } else if (typeClass.equals(Short.class)) {
            return (T) Short.valueOf(ByteBuffer.wrap(bytes).getShort());
        } else if (typeClass.equals(Float.class)) {
            return (T) Float.valueOf(ByteBuffer.wrap(bytes).getFloat());
        } else if (typeClass.equals(Long.class)) {
            return (T) Long.valueOf(ByteBuffer.wrap(bytes).getLong());
        } else {
            // Handle other data types if needed
            return null;
        }
    }

    


    
}