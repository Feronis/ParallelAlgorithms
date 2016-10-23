package sortingBST;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class BSTTraverser implements Serializable {
	
	public BSTTraverser(){
		
	}
	
	public static ArrayList<PairNode> getSortedArray(PairNode root){
		//Check if the current node is empty / null
		//Traverse the left subtree by recursively calling the in-order function.
		//Display the data part of the root (or current node).
		//Traverse the right subtree by recursively calling the in-order function.
		
		ArrayList<PairNode> nodesLeft = new ArrayList<PairNode>();
		ArrayList<PairNode> nodesRight = new ArrayList<PairNode>();
		ArrayList<PairNode> currentNode = new ArrayList<PairNode>();
		if(root.leftChild != null){
			nodesLeft = getSortedArray(root.leftChild);
		}
		currentNode.add(root);
		if(root.rightChild!= null){
			//System.out.println(root.rightChild.getX());
			//try {
			//	Thread.sleep(100);
			//} catch (InterruptedException e) {
			//	// TODO Auto-generated catch block
			//	e.printStackTrace();
			//}
			
			nodesRight = getSortedArray(root.rightChild);
		}
		
		ArrayList<PairNode> sortedNodes = new ArrayList<PairNode>();
		sortedNodes.addAll(nodesLeft);
		sortedNodes.addAll(currentNode);
		sortedNodes.addAll(nodesRight);
		
		return sortedNodes;
		
	}
	
	public static PairNode getBST(PairNode[] nodes){
		if(nodes.length < 1){
			return new PairNode(0,0);
		}
		if(nodes.length == 1){
			return nodes[0];
		}
		int middle = (nodes.length)/2;
		PairNode root = nodes[middle];
		PairNode leftChild = getBST(Arrays.copyOfRange(nodes, 0, nodes.length/2-1));
		PairNode rightChild = getBST(Arrays.copyOfRange(nodes,nodes.length/2+1, nodes.length));
		root.setLeftChild(leftChild);
		root.setRightChild(rightChild);
		return root;
		
	}

}
