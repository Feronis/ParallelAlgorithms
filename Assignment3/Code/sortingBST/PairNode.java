package sortingBST;

import java.io.Serializable;

public class PairNode implements Serializable {
	
	int x;
	int y;
	PairNode leftChild = null;
	PairNode rightChild = null;
	
	public PairNode(int x, int y){
		this.x = x;
		this.y = y;
	}
	
	public void setLeftChild(PairNode left){
		leftChild = left;
	}
	
	public void setRightChild(PairNode right){
		rightChild = right;
	}
	
	public int getX(){
		return x;
	}
	
	public int getY(){
		return y;
	}
	
	public PairNode getLeft(){
		return leftChild;
	}
	
	public PairNode getRight(){
		return rightChild;
	}

}
