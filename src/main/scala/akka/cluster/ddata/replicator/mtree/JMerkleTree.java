package akka.cluster.ddata.replicator.mtree;

import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.bouncycastle.util.Arrays;
import org.bouncycastle.util.encoders.Hex;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

//https://www.pranaybathini.com/2021/05/merkle-tree.html
public class JMerkleTree {

    static byte[] generateHash(byte[] original) {
        return new Keccak.Digest256().digest(original);
    }

    public static class Node {
        private Node left;
        private Node right;
        private byte[] hash;
        public Node(Node leftChild, Node rightChild, byte[] hash) {
            this.left = leftChild;
            this.right = rightChild;
            this.hash = hash;
        }

        Node getLeft() {
            return left;
        }

        Node getRight() {
            return right;
        }

        public byte[] getHash()  {
            return hash;
        }

        public String getHashStr() {
            return new String(Hex.encode(hash), StandardCharsets.UTF_8);
        }
    }

    public static Node treeFromScala(Object[] dataBlocks, Function<byte[], byte[]> genHash) {
        List<Node> childNodes = new ArrayList<>(dataBlocks.length);
        for (int i = 0; i < dataBlocks.length; i++) {
            byte[] block = (byte[]) dataBlocks[i];
            childNodes.add(new Node(null, null, genHash.apply(block)));
        }
        return buildTree(childNodes);
    }

    public static Node generateTree(List<byte[]> dataBlocks) {
        List<Node> childNodes = new ArrayList<>(dataBlocks.size());
        for (byte[] block : dataBlocks) {
            childNodes.add(new Node(null, null, generateHash(block)));
        }
        return buildTree(childNodes);
    }

    static Node buildTree(List<Node> children) {
        List<Node> parents = new ArrayList<>();

        while (children.size() != 1) {
            int index = 0, length = children.size();
            while (index < length) {
                Node leftChild = children.get(index);
                Node rightChild = null;

                if ((index + 1) < length) {
                    rightChild = children.get(index + 1);
                } else {
                    rightChild = new Node(null, null, leftChild.getHash());
                }

                byte[] parentHash = generateHash(Arrays.concatenate(leftChild.getHash(), rightChild.getHash()));
                parents.add(new Node(leftChild, rightChild, parentHash));

                /*System.out.println(
                  new String(Hex.encode(leftChild.getHash()), StandardCharsets.UTF_8) + " + " +
                    new String(Hex.encode(rightChild.getHash()), StandardCharsets.UTF_8) + " = " +
                        new String(Hex.encode(parentHash), StandardCharsets.UTF_8)
                );*/

                index += 2;
            }
            children = parents;
            parents = new ArrayList<>();
        }
        return children.get(0);
    }

    static void printLevelOrderTraversal(Node root) {
        if (root == null) {
            return;
        }

        if ((root.getLeft() == null && root.getRight() == null)) {
            //System.out.println(new String(Hex.encode(root.getHash()), StandardCharsets.UTF_8));
        }
        Queue<Node> queue = new LinkedList<>();
        queue.add(root);
        queue.add(null);

        while (!queue.isEmpty()) {
            Node node = queue.poll();
            if (node != null) {
                System.out.println(new String(Hex.encode(node.getHash()), StandardCharsets.UTF_8));
            } else {
                System.out.println();
                if (!queue.isEmpty()) {
                    queue.add(null);
                }
            }

            if (node != null && node.getLeft() != null) {
                queue.add(node.getLeft());
            }

            if (node != null && node.getRight() != null) {
                queue.add(node.getRight());
            }
        }
    }

    public static void main(String[] args) {
        List<byte[]> dataBlocks = new ArrayList<>();
        dataBlocks.add("Captain America".getBytes(StandardCharsets.UTF_8));
        dataBlocks.add("Iron Man".getBytes(StandardCharsets.UTF_8));
        dataBlocks.add("God of thunder".getBytes(StandardCharsets.UTF_8));
        dataBlocks.add("Doctor strange".getBytes(StandardCharsets.UTF_8));
        dataBlocks.add("dfadsfasgsdg sdfgdfg df g asgfDoctor strange".getBytes(StandardCharsets.UTF_8));
        dataBlocks.add("ertw34t fgsd sdfdfadsfasgsdg sdfgdfg df g asgfDoctor strange".getBytes(StandardCharsets.UTF_8));
        dataBlocks.add("11God of thunder".getBytes(StandardCharsets.UTF_8));
        dataBlocks.add("223Doctor strange".getBytes(StandardCharsets.UTF_8));

        Node root = generateTree(dataBlocks);
        printLevelOrderTraversal(root);
    }
}

