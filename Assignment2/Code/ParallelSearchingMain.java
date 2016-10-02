package spark.test.datastructs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.function.Function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/*
 * ParallelSearching and Inserting Assignment
 * I support the queries of:
 * searching for a specific name, searching within department or major
 * and range queries for favorite number
 * 
 */

public class ParallelSearchingMain {

	public static void main(String[] args) {
		
		//take in data
		ArrayList<Person> people = new ArrayList<Person>();
		try {
			BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Justin Baraboo\\Desktop\\InputData2.txt"));
			String sCurrentLine = br.readLine();
			while (sCurrentLine != null) {
				//System.out.println(sCurrentLine);
				String[] pI= sCurrentLine.split(",");
				Person person = new Person(pI[0],pI[1],pI[2],
						Long.parseLong(pI[3]), Integer.parseInt(pI[4]), Integer.parseInt(pI[5]) );
				people.add(person);
				sCurrentLine = br.readLine();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		//spark stuff
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("ParallelSearchSpark");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		sc.setLogLevel("FATAL");
		
		
		
		//queries for names:
		//idea: hash table as you mostly want to search a specific person
		
		//table properties
		int tablesize = 100;
		Hashtable<Integer,Person> table = new Hashtable<Integer,Person>(tablesize);
		DoubleHasher hasher = new DoubleHasher(table,tablesize);
//		hasher.addElement(people.get(0));
		
		//broadcast table
		JavaRDD<Person> peopleRDD = sc.parallelize(people); 
		final Broadcast<DoubleHasher> sharedTable = sc.broadcast(hasher);
			
		//parallel inserts
		peopleRDD.foreach(person -> {
			DoubleHasher hash = sharedTable.getValue();
			hash.addElement(person);
		});

		
		//parallel searches of names
		ArrayList<String> names = new ArrayList<String>();
		names.add("Justin Baraboo");
		names.add("cats");
//		System.out.println(table.toString());

		JavaRDD<String> namesRDD = sc.parallelize(names);
		namesRDD.foreach(name->{
			DoubleHasher hash = sharedTable.getValue();
			System.out.println("The name " + name + " existing in table is " + hash.searchTable(name));
			//this parallel searches the table in parallel input search.
			//except not really as passing a spark context through here for the searchTablePara 
			//causes some srs seralizability error and I don't want to make spark contexts withing the doublehasher
			//I could try and have a set spark context for the double hasher.
			if(hasher.searchTable(name)){
				Person p = hash.getInfo(name);
				System.out.println(p.getName() + "," + p.getBuilding() + "," + p.getMajor());
			}
		});
		
		//this is the parallelized search
		String name = "Justin Baraboo";
		if(hasher.searchTablePara(name,sc)){
			Person p = hasher.getInfo(name);
			System.out.println(p.getName() + "," + p.getBuilding() + "," + p.getMajor());
		}
				
		//hasher.deletePerson("Justin Baraboo");
		hasher.deletePersonPara("Justin Baraboo", sc);
		System.out.println("The Justin Baraboo existing in table is " + hasher.searchTable("Justin Baraboo"));
	
		//queries for buildings: out of time, will do later
		//idea: make an inverted index of person for buildings and majors
		JavaPairRDD<String, ArrayList<Person>> invertedRDD = peopleRDD.mapToPair(person -> new Tuple2(person.getBuilding(), 
				new ArrayList<Person>().add(person)));
		//reducebyKey to get all buildings with a single arraylist of all persons
		
		
		
		
		
		
		//queries for favorite numbers
		//idea make a binary tree, support range queries		
		JavaPairRDD<Integer,Person> sortedRDD = peopleRDD.mapToPair(person -> new Tuple2(person.getfavNum(), person));
		JavaPairRDD<Integer,Person> sortRDD = sortedRDD.sortByKey();
		List<Tuple2<Integer,Person>> thingtoCheck = sortedRDD.sortByKey().collect();
		
		
		//sort and get rid of mapping
		
	//	List<Tuple2<Person,Integer>> thingtoCheck = sortedRDD.collect();
		for(Tuple2<Integer,Person>thing : thingtoCheck){
			Person pers = (Person)thing._2;
			System.out.println(pers.getfavNum()+ "," + pers.getName());
		}
				
		//do bst creation on sorted array
		int numProcessors = 5;
		int counter = 0;
		ArrayList<Tuple2<Integer,Person>> bst = new ArrayList<Tuple2<Integer,Person>>();
		ArrayList<ArrayList<Tuple2<Integer,Person>>> bsts = new ArrayList<ArrayList<Tuple2<Integer,Person>>>();
		for(int i = 0; i < thingtoCheck.size(); i++){
			if(counter < numProcessors){
				bst.add(thingtoCheck.get(i));
				counter++;
			}
			else{
				bsts.add(bst);
				bst = new ArrayList<Tuple2<Integer,Person>>();
				counter = 0;
			}
		}
		
		ArrayList<Tuple2<Node, Person>> bstRoots = new ArrayList<Tuple2<Node,Person>>();
		
		for(ArrayList<Tuple2<Integer,Person>> bs : bsts){
			bstRoots.add( getBST(  bs ) );
		}
		
		
		JavaRDD<Tuple2<Node,Person>> bstRDD = sc.parallelize(bstRoots);
		
		
		//then do parallell searches
		ArrayList<Integer> favNumArray = new ArrayList<Integer>();
		favNumArray.add(5);
		favNumArray.add(25);
		
		JavaRDD<Integer> favNumRDD = sc.parallelize(favNumArray);
		
		bstRDD.foreach(item->{
			traverse(item._1,5);
			System.out.println("It is  " + traverse(item._1,5) + " that " + 5 + " is in the bst");
		});
		
		
		//parallell adds
		
		//parallel deletions
		

	}
	
	public static Tuple2<Node,Person> getBST(List<Tuple2<Integer,Person>> num){
		if(num.size()==1){
			return new Tuple2(new Node(num.get(0)._1()), num.get(0)._2() );
		}
		else{
			Node root = new Node(num.get(num.size()/2)._1());
			Person rtP = num.get(num.size()/2)._2();
			List<Tuple2<Integer, Person>> firstHalf = num.subList(0, num.size()/2-1);
			List<Tuple2<Integer, Person>> secondHalf = num.subList(num.size()/2+1,num.size()-1);

			Tuple2<Node,Person> left = getBST(firstHalf);
			Tuple2<Node,Person> right = getBST(secondHalf);
			root.setLeft(left._1);
			root.setRight(right._1);
			return new Tuple2(root,rtP);
		}
	}
	
	public static boolean traverse(Node root, int num){
		if(root.value==num){
			return true;
		}
		else if(root.value > num){
			if(root.getLeft() == null){
				return false;
			}
			else{
				traverse(root.getLeft(), num);
			}
		}
		else if(root.value< num){
			if(root.getRight() == null){
				return false;
			}
			else{
				traverse(root.getRight(),num);
			}
		}
		return false;
		
	}

}
