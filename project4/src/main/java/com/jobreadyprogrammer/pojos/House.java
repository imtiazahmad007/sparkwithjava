package com.jobreadyprogrammer.pojos;

import java.util.Date;

public class House {
	
	private int id;
	private String address;
	private int sqft;
	private double price;
	private Date vacantBy;
	

	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public String getAddress() {
		return address;
	}
	
	public void setAddress(String address) {
		this.address = address;
	}
	
	public int getSqft() {
		return sqft;
	}
	public void setSqft(int sqft) {
		this.sqft = sqft;
	}
	
	public double getPrice() {
		return price;
	}


	public void setPrice(double price) {
		this.price = price;
	}
	
	public Date getVacantBy() {
		return vacantBy;
	}
	
	public void setVacantBy(Date vacantBy) {
		this.vacantBy = vacantBy;
	}



	
	
}
