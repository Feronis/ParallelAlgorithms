package spark.test.datastructs;

import java.io.Serializable;
import java.util.ArrayList;

//needs a consistent function

public class MultiNode implements Serializable {
	
	private ArrayList<Integer> elements = new ArrayList<Integer>();
	private ArrayList<MultiNode> pointers = new ArrayList<MultiNode>();
	private int maxElements;
	private int maxKeys;
	private boolean isLeaf = false;
	
	public MultiNode(int maxN, int maxK){
		maxElements = maxN;
		maxKeys = maxK;
	}
	
	public boolean isFull(){
		if (elements.size()== maxElements){
			return true;
		}
		else{
			return false;
		}
	}
	
	public Boolean isNotLeaf(){
		return !isLeaf;
	}
	
	public void setLeaf(boolean e){
		isLeaf = e;
	}
	
	public void addElement(int ele){
			elements.add(ele);	
	}
	
	public void addManyElements(ArrayList<Integer>ele){
		for(int i : ele){
			elements.add(i);
		}
	}
	
	public void addKey(MultiNode node){
		if(pointers.size() < maxKeys){
			pointers.add(node);
		}
		else{
			//throw error
		}
	}
	
	public void addManyKey(ArrayList<MultiNode> nodes){
		for(MultiNode i: nodes){
			pointers.add(i);
		}
	}
	
	public ArrayList<Integer> getElements(){
		return elements;
	}
	
	public ArrayList<Integer> grabLowerHalfElements(){
		ArrayList<Integer> element = new ArrayList<Integer>();
		for(int i = 0; i < maxElements/2; i++){
			//System.out.println("gonna add" + elements.get(i));
			element.add(elements.get(i));
		}
		return element;
	}
	
	public ArrayList<MultiNode> grabLowerHalfKeys(){
		ArrayList<MultiNode> element = new ArrayList<MultiNode>();
		for(int i = 0; i < maxElements/2; i++){
			element.add(pointers.get(i));
		}
		return element;
	}
	
	public ArrayList<Integer> grabUpperHalfElements(){
		ArrayList<Integer> element = new ArrayList<Integer>();
		for(int i = (int) (Math.ceil(maxElements/2.0)); i < maxElements; i++){
			//System.out.println("gonna add" + elements.get(i));
			element.add(elements.get(i));
		}
		return element;
	}
	
	public ArrayList<MultiNode> grabUpperHalfKeys(){
		ArrayList<MultiNode> element = new ArrayList<MultiNode>();
		for(int i = 0; i < Math.ceil(maxKeys/2.0); i++){
			element.add(pointers.get(pointers.size() - i-1));
		}
		return element;
	}
	
	//assuming you have elements
	public int getMin(){
		int min = elements.get(0);
		for(int i : elements){
			if(i < min){
				min = i;
			}
		}
		return min;
	}
	
	public int getMinRemove(){
		int min = elements.get(0);
		for(int i : elements){
			if(i < min){
				min = i;
			}
		}
		elements.remove(elements.indexOf(min));
		return min;
	}
	
	//assuming you have elements
	public int getMax(){
		int max = elements.get(0);
		for(int i : elements){
			if(i > max){
				max = i;
			}
		}
		return max;
	}
	
	public void removeKey(MultiNode node){
		for(MultiNode n : pointers){
			if(n.equals(node)){
				pointers.remove(n);
			}
		}
	}
	
	public ArrayList<MultiNode> getKeys(){
		return pointers;
	}
	
	public int elementsize(){
		return elements.size();
	}
	
	public void removeElements(){
		elements = new ArrayList<Integer>();
	}
	public void removePointers(){
		pointers = new ArrayList<MultiNode>();
	}
}
