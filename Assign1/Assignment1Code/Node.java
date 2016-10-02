package spark.test.datastructs;

import java.io.Serializable;

public class Node implements Serializable {
	
	int value;
	Node lC;
	Node rC;
	
	public Node(int value){
		this.value = value;
	}
	
	public void setLeft(Node left){
		lC = left;
	}
	
	public void setRight(Node right){
		rC = right;
	}
	
	public void setValue(int val){
		value = val;
	}
	
	public Node getLeft(){
		return lC;
	}
	
	public Node getRight(){
		return rC;
	}
	
	public int getValue(){
		return value;
	}
	
	

}
