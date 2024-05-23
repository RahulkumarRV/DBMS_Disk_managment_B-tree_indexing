package index.bplusTree;

import storage.AbstractFile;

import java.util.Queue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/*
    * Tree is a collection of BlockNodes
    * The first BlockNode is the metadata block - stores the order and the block_id of the root node

    * The total number of keys in all leaf nodes is the total number of records in the records file.
*/

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;

    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {
        
        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return (orderBytes[0] << 8) | (orderBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    public int compare(T key1, T key2) {
        if (key1 instanceof Comparable && key2 instanceof Comparable) {
            Comparable<T> comparableKey1 = (Comparable<T>) key1;
            return comparableKey1.compareTo(key2);
        }
        // Default comparison result if Comparable interface is not implemented
        return key1.toString().compareTo(key2.toString());
    }

    private int findLeafNode(T key){
        int rootIndex = this.getRootId();
        InternalNode<T> rootNode = (InternalNode<T>) this.blocks.get(rootIndex);
        T[] keys = rootNode.getKeys();
        int index = 0;
        while(index < rootNode.getNumKeys() && this.compare(keys[index], key) < 0){
            index = index + 1;
        }
        int[] children = rootNode.getChildren();
        if(this.isLeaf(this.blocks.get(children[index]))){
            return children[index];
        }
        else{
            return findLeafNode((InternalNode<T>) this.blocks.get(children[index]), key);
        }
    }

    private int findLeafNode(InternalNode<T> node, T key){
        T[] keys = node.getKeys();
        int index = 0;
        while(index < node.getNumKeys() && this.compare(keys[index], key) < 0){
            index = index + 1;
        }
        int[] children = node.getChildren();
        if(this.isLeaf(this.blocks.get(children[index]))){
            return children[index];
        }
        else{
            return findLeafNode((InternalNode<T>) this.blocks.get(children[index]), key);
        }
          
    }


    private void updateRootIndex(int index){
        byte[] bytes = new byte[2];
        bytes[0] = (byte) (index >> 8);
        bytes[1] = (byte) (index);
        this.blocks.get(0).write_data(2, bytes);
    }

    /*
    split the root node into two leaf nodes and create a new root node pointing to these two new leaf nodes
    */
    private void splitRootAsLeaf(T key, int block_id){
        LeafNode<T> node = (LeafNode<T>) this.blocks.get(this.getRootId());
        int numKeys = node.getNumKeys();
        T[] keys = node.getKeys();
        int[] blocks = node.getBlockIds();
        T[] newKeys = (T[]) new Object[keys.length + 1];
        int[] newblocks = new int[keys.length + 1];
        // find the correct place of the new value
        int index = 0;
        
        while(index < keys.length && this.compare(keys[index], key) < 0){
            index = index + 1;
        }
        System.arraycopy(keys, 0, newKeys, 0, index);
        System.arraycopy(blocks, 0, newblocks, 0, index);
        System.arraycopy(keys, index, newKeys, index + 1, keys.length - index);
        System.arraycopy(blocks, index, newblocks, index + 1, keys.length - index);
        newKeys[index] = key;
        newblocks[index] = block_id;

        int mid = ((this.getOrder() + 1) / 2) - 1;
        // create a new leaf node
        LeafNode<T> sibling = new LeafNode<>(typeClass);
        for(int i=mid; i<newKeys.length; i++){
            sibling.insert(newKeys[i], newblocks[i]);
        }
        // left part
        // LeafNode<T> siblingl = new LeafNode<>(typeClass);
        // for(int i=0; i<mid; i++){
        //     siblingl.insert(newKeys[i], newblocks[i]);
        // }

        node.removeAllKeysFrom(mid);

        T[] keys2 = node.getKeys();
        
        sibling.setNext(node.next());
        sibling.setPrev(this.getRootId());
        this.blocks.add(sibling);
        int siblingIndex = this.blocks.size() - 1;
        if( node.next() != 0){
            ((LeafNode<T>) this.blocks.get((node.next()))).setPrev(siblingIndex);
        }
        node.setNext(siblingIndex);
        InternalNode<T> newRoot = new InternalNode<>(newKeys[mid], this.getRootId(), siblingIndex, typeClass);
        this.blocks.add(newRoot);
        int newRootIndex = this.blocks.size() - 1;
        this.updateRootIndex(newRootIndex);
        node.setParentIndex(newRootIndex);
        sibling.setParentIndex(newRootIndex);
    }

    private void splitNode(int nodeIndex, T key, int block_id){

        if(this.getRootId() == nodeIndex){
            InternalNode<T> node = (InternalNode<T>) this.blocks.get(nodeIndex);
            T[] keys = node.getKeys();
            int[] children = node.getChildren();
            T[] newKeys = (T[]) new Object[keys.length + 1];
            int[] newchildren = new int[children.length + 1];
            // find the correct place of the new value
            int index = 0;
            while(index < keys.length && this.compare(keys[index], key) < 0){
                index = index + 1;
            }
            System.arraycopy(keys, 0, newKeys, 0, index);
            System.arraycopy(children, 0, newchildren, 0, index+1);
            System.arraycopy(keys, index, newKeys, index + 1, keys.length - index);
            System.arraycopy(children, index+1, newchildren, index + 2, children.length - index - 1);
            newKeys[index] = key;
            newchildren[index+1] = block_id;

            int mid = ((this.getOrder() + 1) / 2) - 1;
            InternalNode<T> newSibling = new InternalNode<>(newKeys[mid+1], newchildren[mid+1], newchildren[mid+2], typeClass);
            for(int i= mid+2; i<newKeys.length; i++){
                newSibling.insert(newKeys[i], newchildren[i+1]);
            }

            for(int i= mid; i<newKeys.length; i++){
                if(isLeaf(this.blocks.get(newchildren[i+1]))){
                    ((LeafNode<T>) this.blocks.get(newchildren[i+1])).setParentIndex(this.blocks.size());
                }else{
                    ((InternalNode<T>) this.blocks.get(newchildren[i+1])).setParentIndex(this.blocks.size());
                }
            }
            node.removeAllKeysFrom(mid);
            
            this.blocks.add(newSibling);
            int siblingIndex = this.blocks.size() - 1;
            InternalNode<T> newRoot = new InternalNode<>(newKeys[mid], nodeIndex, siblingIndex, typeClass);
            this.blocks.add(newRoot);
            updateRootIndex(this.blocks.size() - 1);
            node.setParentIndex(this.blocks.size() - 1);
            newSibling.setParentIndex(this.blocks.size() - 1);

        }
        else if(this.isLeaf(this.blocks.get(nodeIndex))){
            LeafNode<T> node = (LeafNode<T>) this.blocks.get(nodeIndex);
            int numKeys = node.getNumKeys();
            T[] keys = node.getKeys();
            int[] blocks = node.getBlockIds();
            T[] newKeys = (T[]) new Object[keys.length + 1];
            int[] newblocks = new int[keys.length + 1];
            // find the correct place of the new value
            int index = 0;
            while(index < keys.length && this.compare(keys[index], key) < 0){
                index = index + 1;
            }
            System.arraycopy(keys, 0, newKeys, 0, index);
            System.arraycopy(blocks, 0, newblocks, 0, index);
            System.arraycopy(keys, index, newKeys, index + 1, keys.length - index);
            System.arraycopy(blocks, index, newblocks, index + 1, keys.length - index);
            
            
            newKeys[index] = key;
            newblocks[index] = block_id;

            int mid = ((this.getOrder() + 1) / 2) - 1;
            // create a new leaf node
            LeafNode<T> sibling = new LeafNode<>(typeClass);
            for(int i=mid; i<newKeys.length; i++){
                sibling.insert(newKeys[i], newblocks[i]);
            }

            node.removeAllKeysFrom(mid);
            sibling.setNext(node.next());
            sibling.setPrev(nodeIndex);
            this.blocks.add(sibling);
            int siblingIndex = this.blocks.size() - 1;
            if( node.next() != 0){
                ((LeafNode<T>) this.blocks.get((node.next()))).setPrev(siblingIndex);
            }
            node.setNext(siblingIndex);
            sibling.setParentIndex(node.getParentIndex());
            if(!this.isFull(node.getParentIndex())){
                ((InternalNode<T>) this.blocks.get(node.getParentIndex())).insert(newKeys[mid], siblingIndex);
                return;
            }
            else{
                splitNode(sibling.getParentIndex(), newKeys[mid], siblingIndex);
            }
        }
        else{
            InternalNode<T> node = (InternalNode<T>) this.blocks.get(nodeIndex);
            T[] keys = node.getKeys();
            int[] children = node.getChildren();
            T[] newKeys = (T[]) new Object[keys.length + 1];
            int[] newchildren = new int[children.length + 1];
            // find the correct place of the new value
            int index = 0;
            while(index < keys.length && this.compare(keys[index], key) < 0){
                index = index + 1;
            }
            System.arraycopy(keys, 0, newKeys, 0, index);
            System.arraycopy(children, 0, newchildren, 0, index+1);
            System.arraycopy(keys, index, newKeys, index + 1, keys.length - index);
            System.arraycopy(children, index+1, newchildren, index + 2, children.length - index - 1);
            newKeys[index] = key;
            newchildren[index+1] = block_id;

            int mid = ((this.getOrder() + 1) / 2) - 1;
            InternalNode<T> newSibling = new InternalNode<>(newKeys[mid+1], newchildren[mid+1], newchildren[mid+2], typeClass);
            node.removeAllKeysFrom(mid);
            newSibling.setParentIndex(node.getParentIndex());
            for(int i= mid+2; i<newKeys.length; i++){
                newSibling.insert(newKeys[i], newchildren[i+1]);
                //update the parent pointer of these children
            }
            for(int i= mid; i<newKeys.length; i++){
                if(isLeaf(this.blocks.get(newchildren[i+1]))){
                    ((LeafNode<T>) this.blocks.get(newchildren[i+1])).setParentIndex(this.blocks.size());
                }else{
                    ((InternalNode<T>) this.blocks.get(newchildren[i+1])).setParentIndex(this.blocks.size());
                }
            }
            
            this.blocks.add(newSibling);
            int siblingIndex = this.blocks.size() - 1;
            if(!this.isFull(node.getParentIndex())){
                ((InternalNode<T>) this.blocks.get(node.getParentIndex())).insert(newKeys[mid], siblingIndex);
                    
                return;
            }
            else{
                splitNode(newSibling.getParentIndex(), newKeys[mid], siblingIndex);
               
            }
        }
        
    }

    // will be evaluated
    public void insert(T key, int block_id) {

        /* Write your code here */
        /* get block which can contain this key*/
        int rootIndex = this.getRootId();

        if(this.isLeaf(this.blocks.get(rootIndex))){
            if(!this.isFull(rootIndex)){
                ((LeafNode<T>) this.blocks.get(rootIndex)).insert(key, block_id); 
            }else{
                splitRootAsLeaf(key, block_id);
            }
            return;
        }
        
        int leafNodeIndex = this.findLeafNode(key);
        if(this.isFull(leafNodeIndex)){
            splitNode(leafNodeIndex, key, block_id);
        }
        else{
            ((LeafNode<T>) this.blocks.get(leafNodeIndex)).insert(key, block_id);
        }
        return;
    }

    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {

        /* Write your code here */
        int rootNodeIndex = this.getRootId();
        if (this.isLeaf(rootNodeIndex)){
            T[] keys = ((LeafNode<T>) this.blocks.get(rootNodeIndex)).getKeys();
            int[] children = ((LeafNode<T>) this.blocks.get(rootNodeIndex)).getBlockIds();
            for(int i=0; i<keys.length; i++){
                if(this.compare(key, keys[i]) == 0){
                    return rootNodeIndex;
                }
            }
            return -1;
        }

        int leafNodeIndex = this.findLeafNode(key);
        T[] keys = ((LeafNode<T>) this.blocks.get(leafNodeIndex)).getKeys();
        int[] children = ((LeafNode<T>) this.blocks.get(leafNodeIndex)).getBlockIds();
        for(int i=0; i<keys.length; i++){
            if(this.compare(key, keys[i]) == 0){
               return leafNodeIndex;
            }
        }
        return -1;
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    public List<BlockNode> return_blocks(){
        return this.blocks;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

}