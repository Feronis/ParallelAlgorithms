package spark.test.datastructs;

import java.io.Serializable;

public class Person implements Serializable {
	
	private String name;
	private String major;
	private String building;
	private String department;
	private Long phone;
	private int id;
	private int favNum;

	
	public Person(){
		
	}
	
	public Person(String name, String building, String major, Long phone, int id, int favNum){
		this.name = name;
		this.major = major;
		this.building = building;
		this.department = department;
		this.phone = phone;
		this.id = id;
		this.favNum = favNum;
	}
	
	public int getfavNum(){
		return favNum;
	}
	
	public String getName(){
		return name;
	}
	
	public void setName(String name){
		this.name = name;
	}
	
	public String getMajor(){
		return major;
	}
	
	public void setMajor(String major){
		this.major = major;
	}
	
	public String getBuilding() {
		return building;
	}

	public void setBuilding(String building) {
		this.building = building;
	}

	public String getDepartment() {
		return department;
	}

	public void setDepartment(String department) {
		this.department = department;
	}

	public Long getPhone() {
		return phone;
	}

	public void setPhone(Long phone) {
		this.phone = phone;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	

}
