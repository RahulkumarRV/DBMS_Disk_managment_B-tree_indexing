import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import index.bplusTree.BPlusTreeIndexFile;
import index.bplusTree.BlockNode;
import index.bplusTree.InternalNode;
import index.bplusTree.LeafNode;


public class BPlusTreeTestFinal {
    private int getRootId(BPlusTreeIndexFile<String> b_plus_tree) {
        List<BlockNode> blocks = b_plus_tree.return_blocks();
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id, BPlusTreeIndexFile<String> b_plus_tree){
        List<BlockNode> blocks = b_plus_tree.return_blocks();
        return isLeaf(blocks.get(id));
    }

    
    public void inorderUtil(ArrayList<Integer> inorder, int current, BPlusTreeIndexFile<String> b_plus_tree) {
        
        List<BlockNode> blocks = b_plus_tree.return_blocks();
        if(isLeaf(current, b_plus_tree)) {
            Object[] keys = ((LeafNode<String>) blocks.get(current)).getKeys();
            for (Object key : keys) {
                int intValue = Integer.parseInt((String) key); 
                inorder.add(intValue);
            }
        }
        else {
            int[] children = ((InternalNode<String>) blocks.get(current)).getChildren();
            
            int total = children.length;

            // All the children except the last
            for (int i = 0; i < total - 1; i++)
                inorderUtil(inorder, children[i], b_plus_tree);

            // Print the current node's data
            Object[] keys = ((InternalNode<String>) blocks.get(current)).getKeys();
            for (Object key : keys) {
                int intValue = Integer.parseInt((String) key); 
                inorder.add(intValue);
            }

            // Last child
            inorderUtil(inorder, children[total-1], b_plus_tree);
        }
    }
    public ArrayList<Integer> return_inorder(BPlusTreeIndexFile<String> b_plus_tree) {
        int root = getRootId(b_plus_tree);
        ArrayList<Integer> inorder = new ArrayList<>();
        int current = root;
        boolean done = false;

        inorderUtil(inorder, current, b_plus_tree);
        return inorder;
    }
    
    
    public void preorderUtil(ArrayList<Integer> preorder, int current, BPlusTreeIndexFile<String> b_plus_tree) {
        
        List<BlockNode> blocks = b_plus_tree.return_blocks();
        if(isLeaf(current, b_plus_tree)) {
            Object[] keys = ((LeafNode<String>) blocks.get(current)).getKeys();
            for (Object key : keys) {
                int intValue = Integer.parseInt((String) key); 
                preorder.add(intValue);
            }
        }
        else {

            // Print the current node's data
            Object[] keys = ((InternalNode<String>) blocks.get(current)).getKeys();
            for (Object key : keys) {
                int intValue = Integer.parseInt((String) key); 
                preorder.add(intValue);
            }

            
            int[] children = ((InternalNode<Integer>) blocks.get(current)).getChildren();
            
            
            int total = children.length;
            // All the children except the last
            for (int i = 0; i < total - 1; i++)
                preorderUtil(preorder, children[i], b_plus_tree);

            // Last child
            preorderUtil(preorder, children[total-1], b_plus_tree);
        }
    }
    public ArrayList<Integer> return_preorder(BPlusTreeIndexFile<String> b_plus_tree) {
        int root = getRootId(b_plus_tree);
        ArrayList<Integer> preorder = new ArrayList<>();
        int current = root;
        boolean done = false;

        preorderUtil(preorder, current, b_plus_tree);

        return preorder;
    }


    

    public void testUtilInorder(String table, String column_name, int order, int[] inserts, int[] expected_inorder) {

        try{
            BPlusTreeIndexFile<String> b_plus_tree;
            b_plus_tree = new BPlusTreeIndexFile<>(order, String.class);

            for(int i=0; i<inserts.length; i++) {
                b_plus_tree.insert(""+inserts[i], inserts[i]);
            }

            ArrayList<Integer> inord = return_inorder(b_plus_tree);
            System.out.println(inord.size()+" <==> "+expected_inorder.length);
            assert(inord.size() == expected_inorder.length);

            for(int i = 0; i < inord.size(); i++){
                assert(inord.get(i) == expected_inorder[i]);
            }

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
    }

    public void testUtilPreorder(String table, String column_name, int order, int[] inserts, int[] expected_preorder) {
        
        try{
            BPlusTreeIndexFile<String> b_plus_tree;
            b_plus_tree = new BPlusTreeIndexFile<>(order, String.class);

            for(int i=0; i<inserts.length; i++) {
                b_plus_tree.insert(""+inserts[i], inserts[i]);
            }

            ArrayList<Integer> preord = return_preorder(b_plus_tree);

            assert(preord.size() == expected_preorder.length);

            for(int i = 0; i < preord.size(); i++){
                assert(preord.get(i) == expected_preorder[i]);
            }

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
    }

    public void testUtilBFS(String table, String column_name, int order, int[] inserts, int[] expected_bfs) {

        try{
            BPlusTreeIndexFile<String> b_plus_tree;
            b_plus_tree = new BPlusTreeIndexFile<>(order, String.class);

            for(int i=0; i<inserts.length; i++) {
                b_plus_tree.insert(""+inserts[i], inserts[i]);
            }

            ArrayList<String> bfs = b_plus_tree.return_bfs();

            assert(bfs.size() == expected_bfs.length);

            for(int i = 0; i < bfs.size(); i++){
                assert(Integer.parseInt(bfs.get(i)) == expected_bfs[i]);
            }

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
    }
      

    @Test
    public void test_tree_bfs_index() {
        try {
            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            calciteConnection.create_index("category", "category_id", 3);
            calciteConnection.return_bfs_index("category", "category_id");
            
            ArrayList<Integer> result = calciteConnection.return_bfs_index("category", "category_id");
            
            ArrayList<Integer> expected_result = new ArrayList<Integer>();
            expected_result.add(5);
            expected_result.add(9);
            expected_result.add(3);
            expected_result.add(7);
            expected_result.add(11);
            expected_result.add(13);
            expected_result.add(2);
            expected_result.add(4);
            expected_result.add(6);
            expected_result.add(8);
            expected_result.add(10);
            expected_result.add(12);
            expected_result.add(14);
            expected_result.add(15);
            for(int i = 1 ; i <= 16 ; i ++) {
                expected_result.add(i);
            }
            // 5 9 3 7 11 13 2 4 6 8 10 12 14 15 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16

            // Uncomment this to test the function after implementing it
            assert(result.size() == expected_result.size());
            for(int i = 0; i < result.size(); i++){
                assert(result.get(i) == expected_result.get(i));
            }

            calciteConnection.close();
            
        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test passed :)");
    }
    
    @Test
    public void test_tree_inorder_1() {
        try {
            
            int[] expected_inorder = {'1', '2', '3', '3', '4', '5'};
            int[] expected_preorder = {'3', '1', '2', '3', '4', '5'};
            int[] expected_bfs = {'3', '1', '2', '3', '4', '5'};
            int[] inserts={'1', '2', '3', '4', '5'};
            testUtilInorder("","", 4, inserts, expected_inorder);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause()+"HEREEE");
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_inorder_1 passed :)");
    }

    @Test
    public void test_tree_preorder_1() {
        try {
            
            int[] expected_inorder = {'1', '2', '3', '3', '4', '5'};
            int[] expected_preorder = {'3', '1', '2', '3', '4', '5'};
            int[] expected_bfs = {'3', '1', '2', '3', '4', '5'};
            int[] inserts={'1', '2', '3', '4', '5'};
            testUtilPreorder("","", 4, inserts, expected_preorder);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause()+"HEREEE");
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_preorder_1 passed :)");
    }

    @Test
    public void test_tree_bfs_1() {
        try {
            
            int[] expected_inorder = {'1', '2', '3', '3', '4', '5'};
            int[] expected_preorder = {'3', '1', '2', '3', '4', '5'};
            int[] expected_bfs = {'3', '1', '2', '3', '4', '5'};
            int[] inserts={'1', '2', '3', '4', '5'};
            testUtilBFS("","", 4, inserts, expected_bfs);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause()+"HEREEE");
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_bfs_1 passed :)");
    }

   
    // Order 4 inserting 1,2,3,4,5
    @Test
    public void test_tree_inorder_2() {
        try {

            int[] expected_inorder = {1, 2, 3, 3, 4, 5};
            int[] expected_preorder = {3, 1, 2, 3, 4, 5};
            int[] expected_bfs = {3, 1, 2, 3, 4, 5};
            int[] inserts={1, 2, 3, 4, 5};
            testUtilInorder("","", 4, inserts, expected_inorder);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_inorder_2 passed :)");
    }

    @Test
    public void test_tree_preorder_2() {
        try {

            int[] expected_inorder = {1, 2, 3, 3, 4, 5};
            int[] expected_preorder = {3, 1, 2, 3, 4, 5};
            int[] expected_bfs = {3, 1, 2, 3, 4, 5};
            int[] inserts={1, 2, 3, 4, 5};
            testUtilPreorder("","", 4, inserts, expected_preorder);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_preorder_2 passed :)");
    }

    @Test
    public void test_tree_bfs_2() {
        try {

            int[] expected_inorder = {1, 2, 3, 3, 4, 5};
            int[] expected_preorder = {3, 1, 2, 3, 4, 5};
            int[] expected_bfs = {3, 1, 2, 3, 4, 5};
            int[] inserts={1, 2, 3, 4, 5};
            testUtilBFS("","", 4, inserts, expected_bfs);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_bfs_2 passed :)");
    }

    
    // Order 4 inserting 1,2,3,4,5,6
    @Test
    public void test_tree_inorder_3() {
        try {

            int[] expected_inorder = {1, 2, 3, 4, 3, 5, 5, 6};
            int[] expected_preorder = {3, 5, 1, 2, 3, 4, 5, 6};
            int[] expected_bfs = {3, 5, 1, 2, 3, 4, 5, 6};
            int[] inserts={1, 2, 3, 4, 5, 6};
            testUtilInorder("","", 4, inserts, expected_inorder);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_inorder_3 passed :)");
    }

    @Test
    public void test_tree_preorder_3() {
        try {

            int[] expected_inorder = {1, 2, 3, 4, 3, 5, 5, 6};
            int[] expected_preorder = {3, 5, 1, 2, 3, 4, 5, 6};
            int[] expected_bfs = {3, 5, 1, 2, 3, 4, 5, 6};
            int[] inserts={1, 2, 3, 4, 5, 6};
            testUtilPreorder("","", 4, inserts, expected_preorder);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_preorder_3 passed :)");
    }

    @Test
    public void test_tree_bfs_3() {
        try {

            int[] expected_inorder = {1, 2, 3, 4, 3, 5, 5, 6};
            int[] expected_preorder = {3, 5, 1, 2, 3, 4, 5, 6};
            int[] expected_bfs = {3, 5, 1, 2, 3, 4, 5, 6};
            int[] inserts={1, 2, 3, 4, 5, 6};
            testUtilBFS("","", 4, inserts, expected_bfs);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_bfs_3 passed :)");
    }

    
    // Order 4 inserting 1,2,3,4,5,7,8
    @Test
    public void test_tree_inorder_4() {
        try {

            int[] expected_inorder = {1, 2, 3, 4, 5, 6, 3, 5, 7, 7, 8};
            int[] expected_preorder = {3, 5, 7, 1, 2, 3, 4, 5, 6, 7, 8};
            int[] expected_bfs = {3, 5, 7, 1, 2, 3, 4, 5, 6, 7, 8};
            int[] inserts={1, 2, 3, 4, 5, 6, 7, 8};
            testUtilInorder("","", 4, inserts, expected_inorder);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_inorder_4 passed :)");
    }

    @Test
    public void test_tree_preorder_4() {
        try {

            int[] expected_inorder = {1, 2, 3, 4, 5, 6, 3, 5, 7, 7, 8};
            int[] expected_preorder = {3, 5, 7, 1, 2, 3, 4, 5, 6, 7, 8};
            int[] expected_bfs = {3, 5, 7, 1, 2, 3, 4, 5, 6, 7, 8};
            int[] inserts={1, 2, 3, 4, 5, 6, 7, 8};
            testUtilPreorder("","", 4, inserts, expected_preorder);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_preorder_4 passed :)");
    }

    @Test
    public void test_tree_bfs_4() {
        try {

            int[] expected_inorder = {1, 2, 3, 4, 5, 6, 3, 5, 7, 7, 8};
            int[] expected_preorder = {3, 5, 7, 1, 2, 3, 4, 5, 6, 7, 8};
            int[] expected_bfs = {3, 5, 7, 1, 2, 3, 4, 5, 6, 7, 8};
            int[] inserts={1, 2, 3, 4, 5, 6, 7, 8};
            testUtilBFS("","", 4, inserts, expected_bfs);

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test test_tree_bfs_4 passed :)");
    }

   
    // Order 3 inserting 1,2,3,4
    @Test
    public void test_tree_inorder_5() {
        try {

            int[] expected_inorder = {1, 2, 2, 3, 3, 4};
            int[] expected_preorder = {2, 3, 1, 2, 3, 4};
            int[] expected_bfs = {2, 3, 1, 2, 3, 4};
            int[] inserts={1, 2, 3, 4};
            testUtilInorder("","", 3, inserts, expected_inorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_inorder_5 passed :)");
    }

    @Test
    public void test_tree_preorder_5() {
        try {

            int[] expected_inorder = {1, 2, 2, 3, 3, 4};
            int[] expected_preorder = {2, 3, 1, 2, 3, 4};
            int[] expected_bfs = {2, 3, 1, 2, 3, 4};
            int[] inserts={1, 2, 3, 4};
            testUtilPreorder("","", 3, inserts, expected_preorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

            int[] expected_inorder1 = {1, 2, 2, 3, 3, 4, 4, 5};
            int[] expected_preorder1 = {3, 2, 1, 2, 4, 3, 4, 5};

        System.out.println("Test test_tree_preorder_5 passed :)");
    }

    @Test
    public void test_tree_bfs_5() {
        try {

            int[] expected_inorder = {1, 2, 2, 3, 3, 4};
            int[] expected_preorder = {2, 3, 1, 2, 3, 4};
            int[] expected_bfs = {2, 3, 1, 2, 3, 4};
            int[] inserts={1, 2, 3, 4};
            testUtilBFS("","", 3, inserts, expected_bfs);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }


        System.out.println("Test test_tree_bfs_5 passed :)");
    }

   
    // Order 3 inserting 1,2,3,4,5
    @Test
    public void test_tree_inorder_6() {

        try {

            int[] expected_inorder = {1, 2, 2, 3, 3, 4, 4, 5};
            int[] expected_preorder = {3, 2, 1, 2, 4, 3, 4, 5};
            int[] expected_bfs = {3, 2, 4, 1, 2, 3, 4, 5};
            int[] inserts={1, 2, 3, 4, 5};
            testUtilInorder("","", 3, inserts, expected_inorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_inorder_6 passed :)");
    }

    @Test
    public void test_tree_preorder_6() {

        try {

            int[] expected_inorder = {1, 2, 2, 3, 3, 4, 4, 5};
            int[] expected_preorder = {3, 2, 1, 2, 4, 3, 4, 5};
            int[] expected_bfs = {3, 2, 4, 1, 2, 3, 4, 5};
            int[] inserts={1, 2, 3, 4, 5};
            testUtilPreorder("","", 3, inserts, expected_preorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_preorder_6 passed :)");
    }

    @Test
    public void test_tree_bfs_6() {

        try {

            int[] expected_inorder = {1, 2, 2, 3, 3, 4, 4, 5};
            int[] expected_preorder = {3, 2, 1, 2, 4, 3, 4, 5};
            int[] expected_bfs = {3, 2, 4, 1, 2, 3, 4, 5};
            int[] inserts={1, 2, 3, 4, 5};
            testUtilBFS("","", 3, inserts, expected_bfs);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_bfs_6 passed :)");
    }

    
    // Order 3 inserting 1,2,3,4,5,6,7
    @Test
    public void test_tree_inorder_7() {

        try {

            int[] expected_inorder = {1, 2, 2, 3, 4, 4, 3, 5, 5, 6, 6, 7};
            int[] expected_preorder = {3, 5, 2, 1, 2, 4, 3, 4, 6, 5, 6, 7};
            int[] expected_bfs = {3, 5, 2, 4, 6, 1, 2, 3, 4, 5, 6, 7};
            int[] inserts={1, 2, 3, 4, 5, 6, 7};
            testUtilInorder("","", 3, inserts, expected_inorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_inorder_7 passed :)");
    }

    @Test
    public void test_tree_preorder_7() {

        try {

            int[] expected_inorder = {1, 2, 2, 3, 4, 4, 3, 5, 5, 6, 6, 7};
            int[] expected_preorder = {3, 5, 2, 1, 2, 4, 3, 4, 6, 5, 6, 7};
            int[] expected_bfs = {3, 5, 2, 4, 6, 1, 2, 3, 4, 5, 6, 7};
            int[] inserts={1, 2, 3, 4, 5, 6, 7};
            testUtilPreorder("","", 3, inserts, expected_preorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_preorder_7 passed :)");
    }

    @Test
    public void test_tree_bfs_7() {

        try {

            int[] expected_inorder = {1, 2, 2, 3, 4, 4, 3, 5, 5, 6, 6, 7};
            int[] expected_preorder = {3, 5, 2, 1, 2, 4, 3, 4, 6, 5, 6, 7};
            int[] expected_bfs = {3, 5, 2, 4, 6, 1, 2, 3, 4, 5, 6, 7};
            int[] inserts={1, 2, 3, 4, 5, 6, 7};
            testUtilBFS("","", 3, inserts, expected_bfs);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_bfs_7 passed :)");
    }


    // Order 3 inserting 1,2,3,1,4,5,6 (Duplicates)
    @Test
    public void test_tree_inorder_8() {

        try {

            int[] expected_inorder = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6};
            int[] expected_preorder = {3, 2, 1, 1, 2, 4, 5, 3, 4, 5, 6};
            int[] expected_bfs = {3, 2, 4, 5, 1, 1, 2, 3, 4, 5, 6};
            int[] inserts={1, 2, 3, 1, 4, 5, 6};
            testUtilInorder("","", 3, inserts, expected_inorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_inorder_8 passed :)");
    }

    @Test
    public void test_tree_preorder_8() {

        try {

            int[] expected_inorder = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6};
            int[] expected_preorder = {3, 2, 1, 1, 2, 4, 5, 3, 4, 5, 6};
            int[] expected_bfs = {3, 2, 4, 5, 1, 1, 2, 3, 4, 5, 6};
            int[] inserts={1, 2, 3, 1, 4, 5, 6};
            testUtilPreorder("","", 3, inserts, expected_preorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_preorder_8 passed :)");
    }

    @Test
    public void test_tree_bfs_8() {

        try {

            int[] expected_inorder = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6};
            int[] expected_preorder = {3, 2, 1, 1, 2, 4, 5, 3, 4, 5, 6};
            int[] expected_bfs = {3, 2, 4, 5, 1, 1, 2, 3, 4, 5, 6};
            int[] inserts={1, 2, 3, 1, 4, 5, 6};
            testUtilBFS("","", 3, inserts, expected_bfs);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_bfs_8 passed :)");
    }

// Order 3 inserting 1,2,3,1,4,5,6,4,7 (Duplicates)
    @Test
    public void test_tree_inorder_9() {

        try {
            int[] expected_inorder = {1, 1, 2, 2, 3, 4, 4, 4, 3, 5, 5, 6, 6, 7};
            int[] expected_preorder = {3, 5, 2, 1, 1, 2, 4, 3, 4, 4, 6, 5, 6, 7};
            int[] expected_bfs = {3, 5, 2, 4, 6, 1, 1, 2, 3, 4, 4, 5, 6, 7};
            int[] inserts={1, 2, 3, 1, 4, 5, 6, 4, 7};
            testUtilInorder("","", 3, inserts, expected_inorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_inorder_9 passed :)");
    }

    @Test
    public void test_tree_preorder_9() {

        try {

            int[] expected_inorder = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6};
            int[] expected_preorder = {3, 5, 2, 1, 1, 2, 4, 3, 4, 4, 6, 5, 6, 7};
            int[] expected_bfs = {3, 5, 2, 4, 6, 1, 1, 2, 3, 4, 4, 5, 6, 7};
            int[] inserts={1, 2, 3, 1, 4, 5, 6, 4, 7};
            testUtilPreorder("","", 3, inserts, expected_preorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_preorder_9 passed :)");
    }

    @Test
    public void test_tree_bfs_9() {

        try {

            int[] expected_inorder = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6};
            int[] expected_preorder = {3, 5, 2, 1, 1, 2, 4, 3, 4, 4, 6, 5, 6, 7};
            int[] expected_bfs = {3, 5, 2, 4, 6, 1, 1, 2, 3, 4, 4, 5, 6, 7};
            int[] inserts={1, 2, 3, 1, 4, 5, 6, 4, 7};
            testUtilBFS("","", 3, inserts, expected_bfs);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_bfs_9 passed :)");
    }


    // Order 5 inserting 11,12,13,14,15,16,17,18,19,20,21,22,23 (Duplicates)
    @Test
    public void test_tree_inorder_10() {

        try {
            int[] expected_inorder = {11, 12, 13, 14, 13, 15, 15, 16, 17, 17, 18, 19, 20, 19, 21, 21, 22, 23};
            int[] expected_preorder = {17, 13, 15, 11, 12, 13, 14, 15, 16, 19, 21, 17, 18, 19, 20, 21, 22, 23};
            int[] expected_bfs = {17, 13, 15, 19, 21, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
            int[] inserts={11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
            testUtilInorder("","", 5, inserts, expected_inorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_inorder_10 passed :)");
    }

    @Test
    public void test_tree_preorder_10() {

        try {

            int[] expected_inorder = {11, 12, 13, 14, 13, 15, 15, 16, 17, 17, 18, 19, 20, 19, 21, 21, 22, 23};
            int[] expected_preorder = {17, 13, 15, 11, 12, 13, 14, 15, 16, 19, 21, 17, 18, 19, 20, 21, 22, 23};
            int[] expected_bfs = {17, 13, 15, 19, 21, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
            int[] inserts={11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
            testUtilPreorder("","", 5, inserts, expected_preorder);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_preorder_10 passed :)");
    }

    @Test
    public void test_tree_bfs_10() {

        try {

            int[] expected_inorder = {11, 12, 13, 14, 13, 15, 15, 16, 17, 17, 18, 19, 20, 19, 21, 21, 22, 23};
            int[] expected_preorder = {17, 13, 15, 11, 12, 13, 14, 15, 16, 19, 21, 17, 18, 19, 20, 21, 22, 23};
            int[] expected_bfs = {17, 13, 15, 19, 21, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
            int[] inserts={11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23};
            testUtilBFS("","", 5, inserts, expected_bfs);

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_bfs_10 passed :)");
    }
 
    @Test
    public void test_tree_delete_1() {

        try {

            int[] expected_bfs = {2, 1, 2};
            int[] inserts={1, 2, 3};

            try{
                BPlusTreeIndexFile<String> b_plus_tree;
                b_plus_tree = new BPlusTreeIndexFile<>(3, String.class);
    
                for(int i=0; i<inserts.length; i++) {
                    b_plus_tree.insert(""+inserts[i], inserts[i]);
                }
                
                boolean ret = b_plus_tree.delete("3");
                assert(ret==true);

                ArrayList<String> bfs = b_plus_tree.return_bfs();
    
                assert(bfs.size() == expected_bfs.length);
    
                for(int i = 0; i < bfs.size(); i++){
                    assert(Integer.parseInt(bfs.get(i)) == expected_bfs[i]);
                }
    
            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

            } catch (Exception e) {
                System.out.println(e);
                System.out.println(e.getCause());
                fail("Exception thrown");
            }

        System.out.println("Test test_tree_bfs_10 passed :)");
    }
 

    
    @Test
    public void test_tree_search_1() {

        try {

            // int[] expected_inorder = {1,2};
            // int[] expected_preorder = {1,2};
            // int[] expected_bfs = {1,2};
            int[] inserts={1,2,3,4,5,6,7};
    
            BPlusTreeIndexFile<String> b_plus_tree;
            b_plus_tree = new BPlusTreeIndexFile<>(3, String.class);

            for(int i=0; i<inserts.length; i++) {
                b_plus_tree.insert(""+inserts[i], inserts[i]);
            }
            
            // int first = BlockNode.counter;

            int id = b_plus_tree.search(""+4);
            List<BlockNode> blocks = b_plus_tree.return_blocks();
            BlockNode node = blocks.get(id);
            // int second = BlockNode.counter;
            // System.out.println("First: "+first+" Second: "+second);
            boolean found=false;
            if (node instanceof LeafNode) {
                LeafNode<String> lf = (LeafNode<String>) node;
                Object[] keys = lf.getKeys(); 
                for (Object key : keys) {
                    Integer intValue = Integer.parseInt((String) key); 
                    // System.out.println(intValue);
                    if(intValue == 4) {
                        found=true;
                        break;
                    } else {
                        System.out.println("No found: "+intValue+intValue.getClass());
                    }
                }
                assert(found);
            } else {
                // throw new IOException("Node is not instance of LeafNode");
                fail("Node is not instance of LeafNode");
            }

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }

        System.out.println("Test test_tree_search_1 passed :)");
    }

    @Test
    public void test_tree_search_2() {

        // try {

            // int[] expected_inorder = {1,2};
            // int[] expected_preorder = {1,2};
            // int[] expected_bfs = {1,2};
            int[] inserts={1,2,3,4,5,6,7};
    
            BPlusTreeIndexFile<String> b_plus_tree;
            b_plus_tree = new BPlusTreeIndexFile<>(3, String.class);

            for(int i=0; i<inserts.length; i++) {
                b_plus_tree.insert(""+inserts[i], inserts[i]);
            }
            
            System.out.println("Starting search");
            int first = BlockNode.counter;
            int id = b_plus_tree.search(""+4);
            int second = BlockNode.counter;
            List<BlockNode> blocks = b_plus_tree.return_blocks();
            BlockNode node = blocks.get(id);
            System.out.println("First: "+first+" Second: "+second);
            boolean found=false;
            if (node instanceof LeafNode) {
                LeafNode<String> lf = (LeafNode<String>) node;
                Object[] keys = lf.getKeys(); 
                for (Object key : keys) {
                    Integer intValue = Integer.parseInt((String) key); 
                    System.out.println(intValue+"(==)");
                    if(intValue == 4) {
                        found=true;
                        break;
                    } else {
                        System.out.println("No found: "+intValue+intValue.getClass());
                    }
                }
                assert(found);
                assert((second-first)<=6);
            } else {
                // throw new IOException("Node is not instance of LeafNode");
                fail("Node is not instance of LeafNode");
            }

        // } catch (Exception e) {
        //     System.out.println(e);
        //     System.out.println(e.getCause());
        //     fail("Exception thrown");
        // }

        System.out.println("Test test_tree_search_2 passed :)");
    }

}
