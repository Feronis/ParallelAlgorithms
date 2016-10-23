package sortingBST;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import spark.test.datastructs.DoubleHasher;
import spark.test.datastructs.Person;

public class BSTSort {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/**
		 * For this sorting module, I am going to be sorting binary search trees where each node has two values
		 * as an ordered pair (x,y) where the trees are originally sorted by x's
		 * 
		 * I want to accomplish 3 sorts:
		 * 1. Sort increasing across all trees such that each tree has roughly same amount of nodes
		 * and largest element in one tree with a root smaller than another is smaller than the smallest element
		 * of another tree with a larger root (that is if you put all the trees side by side you'd have all elements sorted)
		 * 
		 * 2. I want to sort increasing across all trees such that the ys are balanced within a tree, greedily.  That is
		 * if you put all the trees next to each other, it'll still form a total sorted sequence, but the amount of nodes per
		 * tree will no longer be the same.
		 * 
		 * 3. I want to sort across trees such that the ys are balanced and the number of nodes are balanced and each tree
		 * is sorted itself.  That is, we no longer have the entire data set sorted by putting the trees next to each other.
		 */
		
		/**
		 * First, we need a way to generate n random (x,y) pairs and create from them binary search trees.
		 * This is done through the PairBSTCreator which will make nodesPerTree nodes, sort them, and create the balanced bst.
		 */
		int nodesPerTree = 10;
		int numberOfTrees = 5;
		
		ArrayList<PairNode> bstRoots = new ArrayList<PairNode>();
		for(int i = 0; i < numberOfTrees; i++){
		PairBSTCreator creator = new PairBSTCreator();
			PairNode root = creator.createRandomBST(nodesPerTree);
			System.out.println(root.getX() + "," +  root.getY() );
			System.out.println(root.getLeft().getX() + " and the right " + root.getRight().getX());
			bstRoots.add(root);
		}
		
		/**
		 * Now we initialize our spark context so we can communicate to Spark.
		 */
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("ParallelSearchSpark");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("FATAL");
		
		
		/**
		 * 1. To accomplish this sorting, we will do an in order traversal to get a sorted array for reach bst
		 * we will then merge them together.  Finally, we will split it up into numberOfTree chunks again and find
		 * the BSTs again from the sorted sub arrays.
		 */
		
		JavaRDD<PairNode> rootRDD = sc.parallelize(bstRoots);
		//this will transform the roots into sorted lists of nodes by the X value 
		//for each tree, so we will have an RDD of lists
		//granted finding this maybe be able to be done in parallel in parallel, but I am not a clever man.
		JavaRDD<PairNode> arrayRDD = rootRDD.flatMap( root -> (ArrayList<PairNode>)BSTTraverser.getSortedArray(root));
		JavaPairRDD<Integer,PairNode> sortedArrayRDD = 
				 arrayRDD.mapToPair(node -> new Tuple2(node.getX(), node));
		List<Tuple2<Integer,PairNode>> sortedList = sortedArrayRDD.sortByKey().collect();
		arrayRDD.foreach(node -> System.out.println("node X:  " + node.getX()));

		List<PairNode> sortedNodesList = sortedArrayRDD.sortByKey().map(pair -> pair._2).collect();
		
		
		JavaRDD<PairNode> num1RDD = sc.parallelize(sortedNodesList,numberOfTrees);
		
		
		//look into using MapPartition instead
		ArrayList<PairNode> num1Roots = new ArrayList<PairNode>();
		final Broadcast<ArrayList<PairNode>> sharedRoots = sc.broadcast(num1Roots);

		
		num1RDD.foreachPartition(new VoidFunction<Iterator<PairNode>>(){

			@Override
			public void call(Iterator<PairNode> t) throws Exception {
				// TODO Auto-generated method stub
				
				ArrayList<PairNode> rootList = sharedRoots.getValue();
				
				ArrayList<PairNode> nodes = new ArrayList<PairNode>();
				while(t.hasNext()){
					nodes.add(t.next());
				}
				for(PairNode p : nodes){
					System.out.print(p.getX() + ",");
				}
				PairNode root = BSTTraverser.getBST(nodes.toArray(new PairNode[nodes.size()]));
				System.out.println("\n root  " + root.getX());
				System.out.println("__________");

				
				rootList.add(root); //add to arraylist of roots
			}			
		});
		
		
		System.out.println("super cats");
		for(PairNode p: num1Roots){
			System.out.print(p.getX() + ",");
		}
		System.out.println("______");
		
		
		//num1RDD.mapPartitionsWithIndex( ,true);
		
		//make a bst on each partition
		
		
		/*use this as a template
		 * http://www.programcreek.com/java-api-examples/index.php?api=org.apache.spark.api.java.function.FlatMapFunction
		 * 
		  FlatMapFunction<Iterator<Integer>, AvgCount> setup = new FlatMapFunction<Iterator<Integer>, AvgCount>() {
			    @Override
			    public Iterable<AvgCount> call(Iterator<Integer> input) {
			      AvgCount a = new AvgCount(0, 0);
			      while (input.hasNext()) {
			        a.total_ += input.next();
			        a.num_ += 1;
			      }
			      ArrayList<AvgCount> ret = new ArrayList<AvgCount>();
			      ret.add(a);
			      return ret;
			    }
			  };
			  Function2<AvgCount, AvgCount, AvgCount> combine = new Function2<AvgCount, AvgCount, AvgCount>() {
			   @Override
			   public AvgCount call(AvgCount a, AvgCount b) {
			      a.total_ += b.total_;
			      a.num_ += b.num_;
			      return a;
			   }
			  };
		*/
				
		
		/*arrayRDD.mapToPair(new PairFunction<ArrayList<PairNode>, PairNode, Integer>() {
			  public Tuple2<PairNode, Integer> call(ArrayList<PairNode> s) { return new Tuple2<PairNode, Integer>(s.get(0), 1); }
		});*/
		
		//this is for debugging and making sure we are getting the right structure
		/*	List<Object> thing = arrayRDD.collect();
		for(Object i: thing){
			ArrayList<PairNode> what = (ArrayList<PairNode>) i;
			for(PairNode ele : what){
				System.out.println(ele.getX());
			}
			System.out.println("going to another line");
		}*/ //this validates that the sorting works.
		
		
		System.out.println("cats");

	}

}
