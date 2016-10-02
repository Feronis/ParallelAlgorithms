package spark.test.datastructs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class DoubleHasher implements Serializable {
	
	private Hashtable<Integer,Person> table;
	private int size;

	public DoubleHasher(Hashtable<Integer,Person> table,int size){
		this.table = table;
		this.size = size;
	}
	
	public void addElement(Person p){
		
		String s = p.getName();
		//define my own hashing functions probably just some mod stuff
		if(table.size() >= size){
			System.out.println("all full");
			return;
		}
		
		int value = hash1(s);
		if(!table.containsKey(value)){
			table.put(value,p);
		}
		else{
			int i =1;
			value = (hash2(s) + hash1(s))%size;
			while(table.containsKey(value)){
				i++;
				value = (hash2(s)*i + hash1(s))%size;
				if(i == 1000){
					System.out.println("Duh doh, looks like we won't be inserting");
				}
			}
			table.put(value, p);
		}
		
		
		
	}
	
	public boolean searchTable(String s){
		int value = hash1(s);
		if(table.containsKey(value)){
			if(table.get(value).getName().equals(s)){
				return true;
			}
			else{
				int i =1;
				value = (hash2(s) + hash1(s))%size;
				while(table.containsKey(value)){
					if(table.get(value).equals(s)){
						return true;
					}
					i++;
					value = (hash2(s)*i + hash1(s))%size;
					if(i == size*100){
						System.out.println("Duh doh, looks like we won't be finding");
						return false;
					}
				}
								
			}
		}
		return false;
	}
	
	public boolean searchTablePara(String s, JavaSparkContext sc){
		ArrayList<Boolean> answer = new ArrayList<Boolean>();
		answer.add(false);
		ArrayList<Integer> ints = new ArrayList(table.keySet());
		JavaRDD<Integer> keyRDD = sc.parallelize(ints);
		final Broadcast<Hashtable<Integer, Person>> sharedTable = sc.broadcast(table);
		final Broadcast<ArrayList<Boolean>> sharedAnswer = sc.broadcast(answer);
		keyRDD.foreach(key->{
			Hashtable<Integer, Person> table2 = sharedTable.getValue();
			ArrayList<Boolean> ans = sharedAnswer.getValue();
			if(table2.get(key).getName().equals(s)){
				ans.set(0, true);
			}
		});
		
		return answer.get(0);
		
	}
	
	public void deletePerson(String s){
		int value = hash1(s);
		if(table.containsKey(value)){
			if(table.get(value).getName().equals(s)){
				System.out.println("Removing " + table.get(value).getName());
				table.remove(value);
			}
			else{
				int i =1;
				value = (hash2(s) + hash1(s))%size;
				while(table.containsKey(value)){
					if(table.get(value).equals(s)){
						System.out.println("Removing" + table.get(value).getName());
						table.remove(value);
					}
					i++;
					value = (hash2(s)*i + hash1(s))%size;
					if(i == size*100){
						System.out.println("Duh doh, looks like we won't be finding it to delete");
					}
				}
								
			}
		}		
	}
	
	/*
	 * a parallelized delete
	 */
	public void deletePersonPara(String s, JavaSparkContext sc){
		ArrayList<Integer> ints = new ArrayList(table.keySet());
		JavaRDD<Integer> keyRDD = sc.parallelize(ints);
		final Broadcast<Hashtable<Integer, Person>> sharedTable = sc.broadcast(table);		
		keyRDD.foreach(key->{
			Hashtable<Integer, Person> table2 = sharedTable.getValue();
			if(table2.get(key).getName().equals(s)){
				System.out.println("Removing  " + table.get(key).getName());
				table2.remove(key, table2.get(key));
			}
		});
	}
	
	public Person getInfo(String s){
		int value = hash1(s);
		if(table.containsKey(value)){
			if(table.get(value).getName().equals(s)){
				return table.get(value);
			}
			else{
				int i =1;
				value = (hash2(s) + hash1(s))%size;
				while(table.containsKey(value)){
					if(table.get(value).equals(s)){
						return table.get(value);
					}
					i++;
					value = (hash2(s)*i + hash1(s))%size;
					if(i == size*100){
						System.out.println("Duh doh, looks like we won't be finding");
						return null;
					}
				}
								
			}
		}
		return null;
	}		
		
	private int hash1(String s){
	    int strHash = Math.abs(s.hashCode());
	    int hasher = size;
	    //System.out.println("Hasher is:  " + hasher);
	    int value = strHash%hasher;
	    return value;
	}
	
	private int hash2(String s){
	    int strHash = Math.abs(s.hashCode());
	    int hasher = size;
	    int value = (strHash+3)%hasher;
	    return value;
	}
	
}
