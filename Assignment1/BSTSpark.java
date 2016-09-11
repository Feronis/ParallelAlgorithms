package spark.test.datastructs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class BSTSpark {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//starts spark

		//long startTime = System.currentTimeMillis();

		  //note I had to add setMaster("local") to this
		  SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("BSTSpark");
		  JavaSparkContext sc = new JavaSparkContext(sparkConf);
		  sc.setLogLevel("FATAL");
		  
		  int sizeValue = (int) Math.pow(2,5);
		  
		  ArrayList<Integer> nums = new ArrayList<Integer>( sizeValue);
		  ArrayList<Node> nodes = new ArrayList<Node>( sizeValue);
		  
		  nodes.add(new Node(0));
		  
		  for(int i = 1; i < sizeValue; i++){
			  nums.add(i);
			  nodes.add(new Node(i));
		  }
		
		  JavaRDD<Integer> numRDD = sc.parallelize(nums); 

		  final Broadcast<ArrayList<Integer>> broadArray = sc.broadcast(nums);
		  final Broadcast<ArrayList<Node>> nodeArray = sc.broadcast(nodes);


		  numRDD.foreach(item -> {
			  ArrayList<Integer> nArray = broadArray.value();
			  ArrayList<Node> nodeList = nodeArray.value();
			  
			//  System.out.println("test");
			  //for(int i= 1; i <=nums.size();i++) {
				 // System.out.println("Item: " + item.intValue() + "Array " + nArray.get(i-1));

				 // if (item.intValue() == nArray.get(i-1)) {
					  //if i%2 == 0 inner node
					  int i = item.intValue();
					  int n = nums.size()+1;
					  if (i % 2 == 0) {
					  int e = item.intValue();
					  //if greater than half, map it over to its pair on the left to make life easier
					  if(item > n/2){
					  e = e - n/2;
					  e = n/2 - e;
	
					  //need to clean up and comment this code
					  int tempN = n;
					  int tempE = e;
					  while(tempE%2==0 && tempN%2==0){ 
						  tempE = (int) (tempE/gcm(tempE,tempN)); 
					      tempN = (int) (tempN/gcm(tempE,tempN));
					  }
					  if(tempE==1) {
					  int value = e/ 2;
					  int leftChild = item.intValue() - value;
					  int rightChild = item.intValue() + value;
					  nodeList.get(item.intValue()).setLeft(nodeList.get(leftChild));
					  nodeList.get(item.intValue()).setRight(nodeList.get(rightChild));
					  
					 
					  System.out.println("(" + item.intValue() + ", " + leftChild + ", " + rightChild + ")");
					  }
					  else{
						  tempE = tempE/tempN - 2/tempN;
					      int value = (n/tempN)/2;
					      int leftChild = item.intValue() - value;
					      int rightChild = item.intValue() + value;
					      nodeList.get(item.intValue()).setLeft(nodeList.get(leftChild));
						  nodeList.get(item.intValue()).setRight(nodeList.get(rightChild));
					      
					      System.out.println("(" + item.intValue() + ", " + leftChild + ", " + rightChild + ")");
					  }
					 }
					 else {
					 int tempN = n;
					 int tempE = item.intValue();
					 while(tempE%2==0 && tempN%2==0){
						  tempE = (int) (tempE/gcm(tempE,tempN));   ///2;
					      tempN = (int) (tempN/gcm(tempE,tempN));
					 }
					 if(tempE==1) {
						 
						 int value = item.intValue() / 2;
						 int leftChild = item.intValue() - value;
						 int rightChild = item.intValue() + value;
						 nodeList.get(item.intValue()).setLeft(nodeList.get(leftChild));
						 nodeList.get(item.intValue()).setRight(nodeList.get(rightChild));
						  if( (double)item.intValue()/n == .5  ){
						      System.out.println("(" + item.intValue() + " is also root" + ")");
						  }
						 
						 System.out.println("(" + item.intValue() + ", " + leftChild + ", " + rightChild + ")");
					 }
					 else{
						 tempE = tempE/tempN - 2/tempN;
					     int value = (n/tempN)/2;
					     int leftChild = item.intValue() - value;
					     int rightChild = item.intValue() + value;
					     nodeList.get(item.intValue()).setLeft(nodeList.get(leftChild));
						 nodeList.get(item.intValue()).setRight(nodeList.get(rightChild));
					     System.out.println("(" + item.intValue() + ", " + leftChild + ", " + rightChild + ")");
					     }
					 }
				}
				          //else it is leaf
			else{
				//System.out.println("(" +item.intValue() +", "+ 0 +", "+ 0+")");
			}
	//	}
	  //}
	});
		  
	//System.out.println("Cats");
	
	Node root = nodes.get(nodes.size()/2);
	System.out.println(root.getValue());
	System.out.println(root.getLeft().getValue());
	System.out.println(root.getRight().getValue());
	
	ArrayList<Integer> queries = new ArrayList<Integer>();
	queries.add(1);
	queries.add(3);
	queries.add(17);
	JavaRDD<Integer> queryRDD = sc.parallelize(queries);
	final Broadcast<Node> rootB = sc.broadcast(root);

	
	long startTime = System.currentTimeMillis();

/*queryRDD.foreach(item->{
		Node rootS = rootB.getValue();
		if(traverse(rootS, item)){
			System.out.println(item + " is in");
		}
		else{
			System.out.println(item + " is not in");
		}
	});*/
	
	

	
	long endTime   = System.currentTimeMillis();
	long totalTime = endTime - startTime;
	System.out.println(totalTime);
	
	}
	
	public static Boolean traverse(Node r, int val){
		if(val == r.getValue()){
			return true;
		}
		else{
			if(val < r.getValue()){
				if(r.getLeft()==null){
					return false;
				}
				else{
					return traverse(r.getLeft(), val);
				}
			}
			else if(val > r.getValue()){
				if(r.getRight() == null){
					return false;
				}
				else{
					return traverse(r.getRight(), val);
				}
			}
		}
		return false;
	}

	
	//found in: http://stackoverflow.com/questions/6618994/simplifying-fractions-in-java
	public static long gcm(long a, long b) {
	    return b == 0 ? a : gcm(b, a % b);
	}
	
	//traverse tree method
	
	
}

