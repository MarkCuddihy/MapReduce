package mapReduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MapReduce2 {

	public static void main(String[] args) throws IOException {


		////////////
		// INPUT:
		///////////

		//check for args
		if(args.length <= 0){
			System.out.println("No arguments are passed!");
			System.exit(1);
		}
		//store threadCount
		int threadCount = Integer.parseInt(args[0]);
		//create fixed thread pool
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);

		//get input fileNames
		ArrayList<String> inputFileNames = new ArrayList<String>();
		for(int i=1;i<args.length;i++){
			inputFileNames.add(args[i]);
		}

		//map to store file text
		Map<String, String> input = new HashMap<String, String>();

		//store text in map
		for(int i=0;i<inputFileNames.size();i++){
			System.out.println("Reading in: "+inputFileNames.get(i)+"..");
			BufferedReader br = new BufferedReader(new FileReader(inputFileNames.get(i)));
			try{
				StringBuilder sb = new StringBuilder();
				String line = br.readLine();
				while(line!=null){
					sb.append(line.toLowerCase().replaceAll("[^a-zA-Z0-9 -]", ""));
					sb.append("\n");
					line = br.readLine();
				}
				input.put(inputFileNames.get(i), sb.toString());
				System.out.println(inputFileNames.get(i)+" stored!");
			} finally{
				br.close();
			}
		}

		{

			final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

			// MAP:

			final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

			final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
				@Override
				public synchronized void mapDone(String file, List<MappedItem> results) {
					mappedItems.addAll(results);
				}
			};

			List<Future<?>> mapCluster = new ArrayList<Future<?>>(input.size());

			final long startTime = System.currentTimeMillis();

			Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
			while(inputIter.hasNext()) {
				Map.Entry<String, String> entry = inputIter.next();
				final String file = entry.getKey();
				final String contents = entry.getValue();

				Future<?> future = executor.submit(new Runnable() {
					@Override
					public void run() {
						map(file, contents, mapCallback);
					}
				});
				mapCluster.add(future);

			}

			// wait for mapping phase to be over:
			for(Future<?> f : mapCluster) {

				try {
					f.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}

			}

			final long endTime = System.currentTimeMillis();

			final long mapTime= endTime-startTime;

			// GROUP:

			Map<String, List<Future<?>>> groupedItems = Collections.synchronizedMap(new HashMap<String, List<Future<?>>>());

			Iterator<MappedItem> mappedIter = mappedItems.iterator();
			
			//List<Future<?>> groupCluster = new ArrayList<Future<?>>(mappedItems.size());
			
			final GroupCallback<String, String, MappedItem> groupCallback = new GroupCallback<String, String, MappedItem>(){
				
				@Override
				public synchronized void groupDone(String word, String file, List<Future<?>> futures) {
					// TODO Auto-generated method stub
					groupedItems.put(word,futures);

				}
			};

			final long groupStart = System.currentTimeMillis();
			
			synchronized (groupedItems) {


				while(mappedIter.hasNext()) {
					MappedItem item = mappedIter.next();
					String word = item.getWord();
					String file = item.getFile();
					//groupCluster = groupedItems.get(word);
					
					Future<?> future = executor.submit(new Runnable() {
						@Override
						public void run() {
							group(word, file, groupCallback);
						}
					});

					/*if (list == null) {
						list = new LinkedList<String>();
						groupedItems.put(word, list);
					}*/
					groupCluster.add(future);
					
					
				}
			}
			
			// wait for grouping phase to be over:
						for(Future<?> f : list) {

							try {
								f.get();
							} catch (InterruptedException | ExecutionException e) {
								e.printStackTrace();
							}

						}
			
			final long groupEnd = System.currentTimeMillis();
			final long groupTime = groupEnd - groupStart;

			// REDUCE:

			final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
				@Override
				public synchronized void reduceDone(String k, Map<String, Integer> v) {
					output.put(k, v);
				}
			};

			List<Future<?>> reduceCluster = new ArrayList<Future<?>>(groupedItems.size());

			final long startTime2 = System.currentTimeMillis();

			Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
			while(groupedIter.hasNext()) {
				Map.Entry<String, List<String>> entry = groupedIter.next();
				final String word = entry.getKey();
				final List<String> list = entry.getValue();

				Future<?> future = executor.submit(new Runnable() {
					@Override
					public void run() {
						reduce(word, list, reduceCallback);
					}
				});
				reduceCluster.add(future);
			}

			// wait for reducing phase to be over:
			for(Future<?> f : reduceCluster) {
				try {
					f.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}

			//OUTPUT//
			final long endTime2 = System.currentTimeMillis();
			final long reduceTime= endTime2-startTime2;

			//pretty print
			Map<String, Map<String, Integer>> sortedOutput = sortMap(output);
			Arrays.stream(sortedOutput.entrySet().toArray()).forEach(System.out::println);


			shutdownAndAwaitTermination(executor);

			Arrays.stream(groupedItems.entrySet().toArray()).forEach(System.out::println);

			System.out.println("Map time: "+mapTime);
			System.out.println("Reduce time: "+reduceTime);
			System.out.println("Group time: "+groupTime);
		}
	}

	public static void map(String file, String contents, List<MappedItem> mappedItems) {
		String[] words = contents.trim().split("\\s+");
		for(String word: words) {
			mappedItems.add(new MappedItem(word, file));
		}
	}

	public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for(String file: list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		output.put(word, reducedList);
	}

	public static interface MapCallback<E, V> {

		public void mapDone(E key, List<V> values);
	}

	public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
		String[] words = contents.trim().split("\\s+");
		List<MappedItem> results = new ArrayList<MappedItem>(words.length);
		for(String word: words) {
			results.add(new MappedItem(word, file));
		}
		callback.mapDone(file, results);
	}

	public static interface GroupCallback<E,V>{
		
		public void groupDone(E key, List<V> values);
		
	}
	
	public static void group(String word, String file, GroupCallback<String, String, MappedItem> groupCallback) {
		//TODO
		groupCallback.groupDone(word, file);
	}
	
	
	public static interface ReduceCallback<E, K, V> {

		public void reduceDone(E e, Map<K,V> results);
	}

	public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for(String file: list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		callback.reduceDone(word, reducedList);
	}

	private static class MappedItem { 

		private final String word;
		private final String file;

		public MappedItem(String word, String file) {
			this.word = word;
			this.file = file;
		}

		public String getWord() {
			return word;
		}

		public String getFile() {
			return file;
		}

		@Override
		public String toString() {
			return "[\"" + word + "\",\"" + file + "\"]";
		}
	}

	private static void shutdownAndAwaitTermination(ExecutorService pool) {
		pool.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
				pool.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!pool.awaitTermination(60, TimeUnit.SECONDS))
					System.err.println("Pool did not terminate");
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			pool.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

	private static Map<String,Map<String,Integer>> sortMap(Map<String,Map<String,Integer>> map) {

		Map<String,Map<String,Integer>> treeMap = new TreeMap<String,Map<String,Integer>>(map);
		return treeMap;
	}



} 