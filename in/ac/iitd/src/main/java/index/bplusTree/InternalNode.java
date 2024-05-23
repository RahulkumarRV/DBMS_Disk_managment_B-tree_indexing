package index.bplusTree;

/*
    * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
    * Only write code where specified

    * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;
    int parentIndex;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);

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
        int offset = 4;

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
        
        int offset = 4;
        for(int i=0; i<numKeys; i++){
            byte[] keyLengthBytes = this.get_data(offset+2, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            offset += keyLength + 4;
        }
        offset += 2;
        byte[] emtpyIndexBytes = this.get_data(2, 2);
        int nextEmptyIndex = ((emtpyIndexBytes[0] & 0xFF) << 8) | (emtpyIndexBytes[1] & 0xFF);
        byte length[] = new byte[nextEmptyIndex - offset];
        this.write_data(offset, length);
        this.write_data(2, new byte[]{(byte) ((offset) >> 8), (byte) ((offset) & 0xFF)});
        this.write_data(0, new byte[]{(byte) ((numKeys) >> 8), (byte) ((numKeys) & 0xFF)});
    }


    public int getParentIndex() {
        return parentIndex;
    }

    public void setParentIndex(int parentIndex) {
        this.parentIndex = parentIndex;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public void insert(T key, int right_block_id) {
        /* Write your code here */
        int numKeys = this.getNumKeys();
        byte[] emtpyIndexBytes = this.get_data(2, 2);
        int nextEmptyIndex = ((emtpyIndexBytes[0] & 0xFF) << 8) | (emtpyIndexBytes[1] & 0xFF);
        byte[] keyBytes = this.convertTToBytes(key);
        int offset = 4;
        int i = 0;
        for(i = 0; i<numKeys; i++) {
            byte[] keyLengthBytes = this.get_data(offset + 2, 2);
            int keyLength = ((keyLengthBytes[0] & 0xFF) << 8) | (keyLengthBytes[1] & 0xFF);
            T currentKey = this.convertBytesToT(this.get_data(offset + 4, keyLength), typeClass);
            if(this.compare(key, currentKey) < 0){
                break;
            }
            offset += 4 + keyLength;
        }
        offset += 2;
        int keyLength = keyBytes.length;
        
        this.write_data(offset + 4 + keyLength, this.get_data(offset, nextEmptyIndex - offset));

        this.write_data(offset, new byte[]{(byte) (keyLength >> 8), (byte) (keyLength & 0xFF)});
        this.write_data(offset + 2, keyBytes);
        this.write_data(offset + 2 + keyLength, new byte[]{(byte) (right_block_id >> 8), (byte) (right_block_id & 0xFF)});
        numKeys += 1;
        this.write_data(0, new byte[]{(byte) (numKeys >> 8), (byte) (numKeys & 0xFF)});
        nextEmptyIndex += 4 + keyLength;
        this.write_data(2, new byte[]{(byte) (nextEmptyIndex >> 8), (byte) (nextEmptyIndex & 0xFF)});
        
    }



    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        /* Write your code here */
        int numKeys = this.getNumKeys();
        int offset = 4;
        for(int i=0; i < numKeys; i++){
            byte[] keyLengthBytes = this.get_data(offset+2, 2);
            int keyLength = (keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF);
            T currentKey = this.convertBytesToT(this.get_data(offset + 4, keyLength), typeClass);
            if (this.compare(key, currentKey) <= 0){
                int byteLocation = offset + keyLength + 4;
                byte[] childBytes = this.get_data(byteLocation, 2);
                return (childBytes[0] << 8) | (childBytes[1] & 0xFF);
            }else {
                offset += keyLength + 4;
            }
        }
        return -1;
    }

    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];

        /* Write your code here */
        int offset = 4;

        for (int i=0; i <= numKeys; i++){
            byte[] childBlockIdBytes = this.get_data(offset, 2);
            byte[] keyLengthBytes = this.get_data(offset+2, 2);
            int childBlockId = (childBlockIdBytes[0] << 8) | (childBlockIdBytes[1] & 0xFF);
            children[i] = childBlockId;
            offset += 4 + ((keyLengthBytes[0] << 8) | (keyLengthBytes[1] & 0xFF));

        }

        return children;

    }

}
