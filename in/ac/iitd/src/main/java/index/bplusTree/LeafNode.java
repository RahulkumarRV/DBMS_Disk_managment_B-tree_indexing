package index.bplusTree;
import java.nio.ByteBuffer;
/*
    * A LeafNode contains keys and block ids.
    * Looks Like -
    * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
    *
    * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;
    int parentIndex;

    public LeafNode(Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        // default -1 means not any parent
        this.parentIndex = -1;

        return;
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int offset = 8;

        for (int i = 0; i < numKeys; i++) {
            byte[] keyLengthBytes = this.get_data(offset + 2, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            keys[i] = this.convertBytesToT(this.get_data(offset + 4, keyLength), typeClass);
            offset += keyLength + 4;
        }

        return keys;

    }

    // numKeys show after which part we have to remove the keys
    public void removeAllKeysFrom(int numKeys){
        
        int offset = 8;
        for(int i=0; i<numKeys; i++){
            byte[] keyLengthBytes = this.get_data(offset+2, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            offset += keyLength + 4;
        }
        byte[] emtpyIndexBytes = this.get_data(6, 2);
        int nextEmptyIndex = ((emtpyIndexBytes[0] & 0xFF) << 8) | (emtpyIndexBytes[1] & 0xFF);
        byte length[] = new byte[nextEmptyIndex - offset];
        this.write_data(offset, length);
        this.write_data(6, new byte[]{(byte) ((offset) >> 8), (byte) ((offset) & 0xFF)});
        this.write_data(0, new byte[]{(byte) ((numKeys) >> 8), (byte) ((numKeys) & 0xFF)});
    }

    public int getParentIndex() {
        return parentIndex;
    }

    public void setParentIndex(int parentIndex) {
        this.parentIndex = parentIndex;
    }

    public void setPrev(int prevNodeIndex){
        this.write_data(2, new byte[]{(byte) ((prevNodeIndex) >> 8), (byte) ((prevNodeIndex) & 0xFF)});
    }

    public void setNext(int nextNodeIndex){
        this.write_data(4, new byte[]{(byte) ((nextNodeIndex) >> 8), (byte) ((nextNodeIndex) & 0xFF)});
    }

    public int next(){
        byte[] nextNodeBytes = this.get_data(4, 2);
        int nextNodeIndex = ((nextNodeBytes[0] & 0xFF) << 8) | (nextNodeBytes[1] & 0xFF);
        return nextNodeIndex;
    }

    public int prev(){
        byte[] prevNodeBytes = this.get_data(2, 2);
        int prevNodeIndex = ((prevNodeBytes[0] & 0xFF) << 8) | (prevNodeBytes[1] & 0xFF);
        return prevNodeIndex;
    }

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();

        int[] block_ids = new int[numKeys];

        /* Write your code here */

        int offset = 8;
        for (int i = 0; i < numKeys; i++) {
            byte[] keyLengthBytes = this.get_data(offset + 2, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            byte[] blockIdBytes = this.get_data(offset, 2);
            block_ids[i] = ((blockIdBytes[0] & 0xFF) << 8) | (blockIdBytes[1] & 0xFF);
            offset += keyLength + 4;
        }

        return block_ids;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int block_id) {


        /* Write your code here */
        int numKeys = this.getNumKeys();
        byte[] emtpyIndexBytes = this.get_data(6, 2);
        int nextEmptyIndex = ((emtpyIndexBytes[0] & 0xFF) << 8) | (emtpyIndexBytes[1] & 0xFF);
        byte[] keyBytes = this.convertTToBytes(key);
        int keyLength = keyBytes.length;
        int offset = 8;
        int i = 0;
        for(i = 0; i<numKeys; i++){
            T currentKey = this.convertBytesToT(this.get_data(offset + 4, keyLength), typeClass);
            if(this.compare(key, currentKey) < 0){
                break;
            }
            offset += 4 + keyLength;
        }

        this.write_data(offset + 4 + keyLength, this.get_data(offset, nextEmptyIndex - offset));

        this.write_data(offset, new byte[]{(byte) (block_id >> 8), (byte) (block_id & 0xFF)});
        this.write_data(offset + 2, new byte[]{(byte) (keyLength >> 8), (byte) (keyLength & 0xFF)});
        this.write_data(offset + 4, keyBytes);
        numKeys += 1;
        this.write_data(0, new byte[]{(byte) (numKeys >> 8), (byte) (numKeys & 0xFF)});
        nextEmptyIndex += 4 + keyLength;
        this.write_data(6, new byte[]{(byte) (nextEmptyIndex >> 8), (byte) (nextEmptyIndex & 0xFF)});

        return;

    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        /* Write your code here */
        int numKeys = this.getNumKeys();
        int offset = 8;
        for(int i=0; i < numKeys; i++){
            byte[] keyLengthBytes = this.get_data(offset+2, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            T currentKey = this.convertBytesToT(this.get_data(offset + 4, keyLength), typeClass);
            if (this.compare(key, currentKey) <= 0){
                byte[] childBytes = this.get_data(offset, 2);
                return (childBytes[0] << 8) | (childBytes[1] & 0xFF);
            }else {
                offset += keyLength + 4;
            }
        }
        return -1;
    }

}
