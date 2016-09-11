package spark.test.datastructs;

import java.util.ArrayList;
import java.util.Random;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.util.hashing.MurmurHash3;



//learned about bloom filters from:
//https://en.wikipedia.org/wiki/Bloom_filter
//http://billmill.org/bloomfilter-tutorial/
//https://en.wikipedia.org/wiki/MurmurHash#Algorithm

public class BloomFilterSpark {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		 long startTime = System.currentTimeMillis();

		
		 SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("BSTSpark");
		 JavaSparkContext sc = new JavaSparkContext(sparkConf);
		 sc.setLogLevel("FATAL");
		
		 
		 //different data files to grab words.
		 JavaRDD<String> input = sc.textFile("C:\\Users\\Justin Baraboo\\Desktop\\cats.txt");
		 JavaRDD<String> input2 = sc.textFile("C:\\Users\\Justin Baraboo\\Desktop\\1100.txt");		 

		 

		 int m = 700; //number of bits for our bloom filter
		 int k = 4; //number of filters
		 
		 int bloom[] = new int[m];  //all values are default zero which is great
		 int seeds[] = new int[k];

		 Random r = new Random();
		 
		 
		 for(int i = 0; i < k; i++){
			 seeds[i] = r.nextInt(); //sets seeds for hashing
		 }
		 
		 //broadcast variables
		 final Broadcast<int[]> bloomFilter = sc.broadcast(bloom); 
		 final Broadcast<int[]> seedArray = sc.broadcast(seeds);
		 
		 input.foreach(item->{
				MurmurHash3 hasher = new MurmurHash3(); //create a hasher
				int[] seed = seedArray.value();
				int[] blooms = bloomFilter.getValue();
				for(int i = 0; i < seed.length; i++){
					int value = hasher.stringHash(item, seed[i])%m; //hash the values
					if(value <0){
						value = value*-1;
					}
					blooms[value]=1; //set the bit to true
				}
		 });


	    //test it
		JavaRDD<String> testWords = sc.textFile("C:\\Users\\Justin Baraboo\\Desktop\\testWords.txt");
		Accumulator<Integer> inCount = sc.intAccumulator(0);
		Accumulator<Integer> notInCount = sc.intAccumulator(0);
		testWords.foreach(x->{
			String str = x;
			boolean isIn = true;
			for(int i : seeds){
			    MurmurHash3 hasher = new MurmurHash3();
				int value = hasher.stringHash(str, i)%m;
				if(value <0){
					value = value*-1;
				}			
				if(bloom[value] ==0 && isIn){
					isIn = false;
					//System.out.println(str +" is not in");
					notInCount.add(1);
				}
			}
			if(isIn){
				//System.out.println("Maybe " + str + " is in");
				inCount.add(1);
			}
		});

		System.out.println("We thought " + inCount.value() + " might be in while " + notInCount.value() + " are not in");
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
		
	}
	
	
	
	

}
