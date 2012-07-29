package cn.ict.magicube.spikes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class TestReflectTargetClass implements Sum {
	static final Log LOG = LogFactory.getLog(TestReflectTargetClass.class);

	public int getSum(int a, int b) {
		LOG.info(String.format("%d + %d = %d", a, b, a + b));
		return a + b;
	}
	
	static class MyInvocationHandler implements InvocationHandler {
		private final Sum _inner;
		MyInvocationHandler() {
			_inner = new TestReflectTargetClass();
		}
		
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			//return method.invoke(_inner, args);
			if (method.getName() == "getSum") {
				args[1] = new Integer(100);
			}
			return method.invoke(_inner, args);
		}
	}
	
	public static void main(String[] args) {
		MyInvocationHandler handler = new MyInvocationHandler();
		Sum s = (Sum)Proxy.newProxyInstance(Sum.class.getClassLoader(),
				new Class[] { Sum.class }, handler);
		s.getSum(10, 20);
	}
	
}
