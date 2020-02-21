package com.spark.study.core;
/**
 * 自定义二次排序Key
 */
import java.io.Serializable;
import scala.math.Ordered;

public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable{
	private static final long serialVersionUID = 1L;

	//自定义需要排序的列
	private int first;
	private int second;
	
	public SecondarySortKey(int first, int second) {
		this.first = first;
		this.second = second;
	}

	//大于
	@Override
	public boolean $greater(SecondarySortKey that) {
		if(this.first > that.getFirst()) {	//如果第一列大，则返回true
			return true;
		} else if(this.first == that.getFirst() && this.second > that.getSecond()) {//如果第一列相同，第二列大，则返回true
			return true;
		}
		//否则返回false
		return false;
	}
	
	//大于等于
	@Override
	public boolean $greater$eq(SecondarySortKey that) {
		if(this.$greater(that)) {
			return true;
		} else if(this.first == that.getFirst() && this.second == that.getSecond()) {
			return true;
		}
		return false;
	}

	//小于
	@Override
	public boolean $less(SecondarySortKey that) {
		if(this.first < that.getFirst()) {
			return true;
		}else if(this.first == that.getFirst() && this.second < that.getSecond()) {
			return true;
		}
		return false;
	}
	
	//小于等于
	@Override
	public boolean $less$eq(SecondarySortKey that) {
		if(this.$less(that)) {
			return true;
		} else if(this.first == that.getFirst() && this.second == that.getSecond()){
			return true;
		}
		return false;
	}

	@Override
	public int compare(SecondarySortKey that) {
		if(this.first - that.getFirst() != 0) {
			return this.first - that.getFirst();
		} else{
			return this.second - that.getSecond();
		}
	}
	
	@Override
	public int compareTo(SecondarySortKey that) {
		if(this.first - that.getFirst() != 0) {
			return this.first - that.getFirst();
		} else{
			return this.second - that.getSecond();
		}
	}
	
	//为要进行排序的多列，提供getter和setter方法，以及hashcode和equals方法
	public int getFirst() {
		return first;
	}

	public void setFirst(int first) {
		this.first = first;
	}

	public int getSecond() {
		return second;
	}

	public void setSecond(int second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + first;
		result = prime * result + second;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SecondarySortKey other = (SecondarySortKey) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
	
	
	
}
