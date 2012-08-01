package cn.ict.magicube.spikes;

import java.util.ServiceLoader;

public abstract class TestSPI {
	
	public abstract String getName();
	public abstract void work();
	
	public static void main(String[] args) {
		System.out.println("begin");
		ServiceLoader<TestSPI> loader = ServiceLoader.load(TestSPI.class);
		for (TestSPI x : loader) {
			System.out.println(x.getName());
			x.work();
		}
		System.out.println("end");
	} 
}
