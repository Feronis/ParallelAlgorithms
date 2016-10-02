package spark.test.datastructs;

import java.util.ArrayList;
import java.util.Arrays;

/*
 * regular implementation of sorted array to BST
 */

public class BSTRegular {

	
	//24 is highest it can do without going over heap
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		int[] nums = new int[(int) Math.pow(2, 15)];
		for(int i = 1; i < Math.pow(2, 15); i++){
			nums[i] = i;
		}
		
		Node root = new Node(0);
		root = getBST(nums);
		System.out.println(root.getValue());
		
		long startTime = System.currentTimeMillis();
		ArrayList<Integer> queries = new ArrayList<Integer>();
		queries.add(1);
		queries.add(3);
		queries.add(17);
		
		for(int i : queries){
			if(traverse(root, i)){
				System.out.println(i + " is in");
			}
			else{
				System.out.println(i + " is not in");
			}
		}
		
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
				if(r.getLeft() == null){
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
	
	public static Node getBST(int[] num){
		if(num.length==1){
			return new Node(num[0]);
		}
		else{
			Node root = new Node(num[num.length/2]);
			int[] firstHalf = Arrays.copyOfRange(num, 0, num.length/2);
			int[] secondHalf = Arrays.copyOfRange(num,num.length/2, num.length);

			Node left = getBST(firstHalf);
			Node right = getBST(secondHalf);
			root.setLeft(left);
			root.setRight(right);
			return root;
			
		}
	}

}
