package sortingBST;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class PairBSTCreator {
	
	private static final int MAX_X = 255;
	private static final int MAX_Y = 255;

	public PairBSTCreator(){
		
	}
	
	public PairNode createRandomBST(int numNodes){
		PairNode root = null;
		Random r = new Random();
		ArrayList<PairNode> nodes = new ArrayList<PairNode>();
		for(int i = 0; i < numNodes; i++){
			//choose random int values for x
			//choose random int values for y
			int x = r.nextInt(MAX_X);
			int y = r.nextInt(MAX_Y);
			PairNode node = new PairNode(x,y);
			nodes.add(node);
		}
		nodes = selectionSort(nodes);
		root = getBST(nodes.toArray(new PairNode[nodes.size()]));
		
		return root;
	}
	
	private ArrayList<PairNode> selectionSort(ArrayList<PairNode> nodes){
		for(int i = 0; i < nodes.size(); i++ ){
			int min = nodes.get(i).getX();
			int minIndex = i;
			for(int j = i; j < nodes.size(); j++){
				if(nodes.get(j).getX() < min){
					min = nodes.get(j).getX();
					minIndex = j;
				}
			}
			Collections.swap(nodes, i, minIndex);
		}		
		return nodes;
	}
	
	private PairNode getBST(PairNode[] nodes){
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
