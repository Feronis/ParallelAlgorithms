package spark.test.datastructs;

import java.util.Comparator;

import org.apache.spark.api.java.function.Function;

public class IntComparator implements Function, Comparator<Integer> {

	@Override
	public Object call(Object v1) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int compare(Integer o1, Integer o2) {
		if(o1 > o2){
			return o1;
		}
		else{
			return o2;
		}
	}

}
