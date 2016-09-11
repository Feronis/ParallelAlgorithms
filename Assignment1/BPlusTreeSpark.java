package spark.test.datastructs;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class BPlusTreeSpark {

	public static void main(String[] args) {
		//note I had to add setMaster("local") to this
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("BSTSpark");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("FATAL");
		  
		//sort input
		  
		//parameters for B+ tree
		int elementMax = 4;
		int pointerMax = elementMax+1;
		//creating a data set, normally these would be a ptr value that points
		//to some part of the disk as well as being comparable to other values
		//for illustrative purposes I'm just going to use regular ints.
		ArrayList<Integer> num = new ArrayList<Integer>();
		  for(int i =1; i <=24; i++){
			  num.add(i);
		}
		  
		ArrayList<MultiNode> leaves = new ArrayList<MultiNode>();
		  
		//set up how many full leaf nodes we will have.
		for(int i =1; i <=24; i=i+elementMax){
			  leaves.add(new MultiNode(elementMax, pointerMax));
		}
		final Broadcast<ArrayList<MultiNode>> leafArray = sc.broadcast(leaves);

		  
		JavaRDD<Integer> numRDD = sc.parallelize(num,(num.size()+1)/elementMax);
		  JavaRDD<Tuple2<Integer,Integer>> pairedRDD = numRDD.map(item-> new Tuple2(item,(int)Math.ceil(((1.0)*item/elementMax))));	
		  pairedRDD.foreach(x -> {
			  ArrayList<MultiNode> leafs = leafArray.getValue();
			  //System.out.println(x);
			  //System.out.println("x._2 -1 : " + (x._2-1));
			  //System.out.println("x._1 : " + x._1);
			  leafs.get(x._2-1).addElement(x._1);
			  leafs.get(x._2-1).setLeaf(true);
		  });
		  

		  //now combine the leaves
		  MultiNode root = new MultiNode(elementMax,pointerMax);
		  //if only 1 leaf, it is actually root.
		  root.addKey(leaves.get(0));
		  root.addKey(leaves.get(1));
		  root.addElement(leaves.get(1).getMin());
		  System.out.println(leaves.get(1).getMin() + "cats");
		  
		  
		  MultiNode insertNode;
		  for(int i =2; i < leaves.size(); i++){
			  //traverse tree to see where you need to insert
			  //use min value to traverse
			  int min = leaves.get(i).getMin();
			  //see if you can insert into that layer
			  insertNode = traverseUpper(root, min);
			  if(!insertNode.isFull()){
				  insertNode.addKey(leaves.get(i));
				  insertNode.addElement(leaves.get(i).getMin());
			  }
			  else{
			  //if not split
				  System.out.println("we split");
				  splitNode(insertNode, root, elementMax, pointerMax);
			  }
		  }
		  
		  System.out.println("These are in root");
		  for(int i : root.getElements()){
			  System.out.println(i + " ");
		  }
		  
		  for(MultiNode n : root.getKeys()){
			  for(int value : n.getElements()){
				  System.out.print(value+" ");
			  }
			  System.out.println("");
			  System.out.println("-----");
		  }
		  for(MultiNode n : root.getKeys()){
			  for(MultiNode m : n.getKeys()){
				  for(int value : m.getElements()){
					  System.out.print(value+" ");
				  }
				  System.out.println("");
				  System.out.println("-----");
			  }
		  }
		  	  
	}
	
	public static MultiNode traverseUpper(MultiNode root, int value){
		ArrayList<Integer> elements = root.getElements();
		int pointer = elements.size();
		for(int i =0; i < elements.size(); i++){
			if(value < elements.get(i)){
				pointer = i;
				i = elements.size();
			}
		}
		System.out.println(root.getElements().size() + "size");
		System.out.println(root.getKeys().size() + "size");
		if(root.getKeys().get(pointer).isNotLeaf()){
			return traverseUpper(root.getKeys().get(pointer), value);
		}
		else{
			return root;
		}
	}
	
	public static void splitNode(MultiNode node, MultiNode root, int k, int v){
		if(node.equals(root)){
			System.out.println("made it here");
			MultiNode leftChild = new MultiNode(k,v);
			MultiNode rightChild = new MultiNode(k,v);
			leftChild.addManyElements(root.grabLowerHalfElements());
			rightChild.addManyElements(root.grabUpperHalfElements());
			leftChild.addManyKey(root.grabLowerHalfKeys());
			leftChild.addManyKey(root.grabUpperHalfKeys());
			root.removeElements();
			root.removePointers();
			root.addKey(leftChild);
			root.addKey(rightChild);
			root.addElement(rightChild.getMinRemove());
		}
		else{
			node.setLeaf(true);
			MultiNode leftChild = new MultiNode(k,v);
			MultiNode rightChild = new MultiNode(k,v);
			leftChild.addManyElements(node.grabLowerHalfElements());
			rightChild.addManyElements(node.grabUpperHalfElements());
			leftChild.addManyKey(node.grabLowerHalfKeys());
			leftChild.addManyKey(node.grabUpperHalfKeys());
			int kickUp = rightChild.getMinRemove();
			MultiNode parent = traverseUpper(root, kickUp);
			
			if(!parent.isFull()){
				parent.addElement(kickUp);
				parent.addKey(leftChild);
				parent.addKey(rightChild);
				parent.removeKey(node);
			}
			else{
				splitNode(parent, root,k,v);
				MultiNode newParent = traverseUpper(root, kickUp);
				newParent.addElement(kickUp);
				newParent.addKey(leftChild);
				newParent.addKey(rightChild);
				newParent.removeKey(node);
			}
		}
	}

	//gonna return an arrayList<Integer> 1= in 0 = not in
	public void parallelSearch(MultiNode root, ArrayList<Integer> searchables){
		//paralleize array
		//broadcast root
		//for each value navigate through b+ tree
	}
}
