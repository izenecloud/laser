package com.b5m.larser.dispatch;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.swing.tree.DefaultMutableTreeNode;

import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;

public class Pipeline {
	/*private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private final DefaultMutableTreeNode comTree = new DefaultMutableTreeNode();
	private Component root;
	private Map<String, Component> coms = new HashedMap();

	public Pipeline(List<ComponentContext> comList, final Logger log) throws IOException {
		
		Iterator<ComponentContext> iterator = comList.iterator();
		while (iterator.hasNext()) {
			ComponentContext comContext = iterator.next();
			try {
				log.debug("running Component {}", comContext.toJson());
				Component com = comContext.newInstance();
				put(com);
				if (!iterator.hasNext()) {
					root = com;
				}
			} catch (Exception e) {
				e.printStackTrace();
				log.error("Component {} is failed for {}", comContext.toJson(),
						e.getMessage());
				throw new IOException(e.getCause());
			}
		}
		if (!validate()) {
			String errMsg = new String("Configure file Illegal");
			log.error(errMsg);
			throw new IOException(errMsg);
		}
	}
	
	private void put(Component com) {
		coms.put(com.context().getName(), com);
	}
	
	private boolean validate() {
		comTree.setUserObject(root);
		addChildRecursive(comTree);
		coms = null;
		return true;
	}
	
	public boolean addChildRecursive(DefaultMutableTreeNode root) {
		Component rootCom = (Component)root.getUserObject();
		List<String> inputList = rootCom.context().getInputList();
		for (String input : inputList) {
			Component com = coms.get(input);
			if (null == com) {
				return false;
			}
			DefaultMutableTreeNode node = new DefaultMutableTreeNode(com);
			addChildRecursive(node);
			comTree.add(node);
		}
		return true;
	}
	
	private void executeKeyRecursive(Request req, Long expire, DefaultMutableTreeNode root) {
		DefaultMutableTreeNode child = (DefaultMutableTreeNode)root.getFirstChild();
		if (null == child) {
			Component com = (Component)root.getUserObject();
			com.generateKey(req, comTree);
			return;
		}
		executeKeyRecursive(req, expire, child);
		while (null != (child = child.getNextSibling())) {
			executeKeyRecursive(req, expire, child);
		}
		Component com = (Component)root.getUserObject();
		com.generateKey(req, comTree);
	}
	
	private void executeValueRecursive(Request req, Long expire, DefaultMutableTreeNode root) {
		DefaultMutableTreeNode child = (DefaultMutableTreeNode)root.getFirstChild();
		if (null == child) {
			Component com = (Component)root.getUserObject();
			com.generateValue(req, comTree);
			return;
		}
		executeValueRecursive(req, expire, child);
		while (null != (child = child.getNextSibling())) {
			executeValueRecursive(req, expire, child);
		}
		Component com = (Component)root.getUserObject();
		com.generateValue(req, comTree);
	}
	
	public String generateKey(final Request request, final Long expire) throws TimeoutException, InterruptedException {
		//TODO multi-thread
		Runnable generater = new Runnable() {
			public void run() {
				executeKeyRecursive(request, expire, comTree);				
			}
		};
		executor.execute(generater);
		if(!executor.awaitTermination(expire, TimeUnit.MILLISECONDS)) {
			throw new TimeoutException("Timeout in generate response");
		}
		return root.context().getResponse().getKey();
	}
	
	public Response generateValue(final Request request, final Long expire) throws TimeoutException, InterruptedException {
		//TODO multi-thread
		Runnable generater = new Runnable() {
			public void run() {
				executeValueRecursive(request, expire, comTree);				
			}
		};
		executor.execute(generater);
		if(!executor.awaitTermination(expire, TimeUnit.MILLISECONDS)) {
			throw new TimeoutException("Timeout in generate response");
		}
		return root.context().getResponse();
	}*/
}
