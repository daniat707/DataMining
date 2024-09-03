import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Coordinator {

    private List<String> inputData;
    private Map<String, List<String>> resultsMap;

    public Coordinator(List<String> inputData) {
        this.inputData = inputData;
        this.resultsMap = new TreeMap<>();
    }

    public void execute() {
        int numMapNodes = 2; // Número de hilos para map
        int numReduceNodes = 1; // Número de hilos para reduce

        // Fase de Map
        ExecutorService mapPool = Executors.newFixedThreadPool(numMapNodes);
        List<Future<Map<String, List<Integer>>>> mapFutures = new ArrayList<>();

        for (String line : inputData) {
            MapNode mapNode = new MapNode(line);
            Future<Map<String, List<Integer>>> future = mapPool.submit(mapNode);
            mapFutures.add(future);
        }

        mapPool.shutdown();
        while (!mapPool.isTerminated()) {}

        // Almacenar e imprimir los resultados de la fase Map
        resultsMap.put("Map", new ArrayList<>());
        for (int i = 0; i < mapFutures.size(); i++) {
            Future<Map<String, List<Integer>>> future = mapFutures.get(i);
            try {
                Map<String, List<Integer>> output = future.get();
                for (Map.Entry<String, List<Integer>> entry : output.entrySet()) {
                    for (Integer count : entry.getValue()) {
                        resultsMap.get("Map").add("(" + entry.getKey() + ", " + count + ")");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Resultado de la fase Map:");
        resultsMap.get("Map").sort(null);
        resultsMap.get("Map").forEach(System.out::println);

        // Fase de Shuffle
        Map<String, List<Integer>> shuffledData = shuffle(mapFutures);

        // Fase de Reduce
        ExecutorService reducePool = Executors.newFixedThreadPool(numReduceNodes);
        Future<Map<String, Integer>> reduceFuture = reducePool.submit(new ReduceNode(shuffledData));

        reducePool.shutdown();
        while (!reducePool.isTerminated()) {}

        // Almacenar e imprimir los resultados de la fase Reduce
        try {
            Map<String, Integer> finalCounts = reduceFuture.get();
            resultsMap.put("Reduce", new ArrayList<>());
            for (Map.Entry<String, Integer> entry : finalCounts.entrySet()) {
                resultsMap.get("Reduce").add("(" + entry.getKey() + ", " + entry.getValue() + ")");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Resultado de la fase Reduce:");
        resultsMap.get("Reduce").forEach(System.out::println);

        System.out.println("MapReduce completado.");
    }

    private Map<String, List<Integer>> shuffle(List<Future<Map<String, List<Integer>>>> mapFutures) {
        Map<String, List<Integer>> shuffled = new TreeMap<>();
        try {
            for (Future<Map<String, List<Integer>>> future : mapFutures) {
                Map<String, List<Integer>> output = future.get();
                for (Map.Entry<String, List<Integer>> entry : output.entrySet()) {
                    shuffled.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(entry.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Almacenar e imprimir el resultado de la fase Shuffle
        resultsMap.put("Shuffle", new ArrayList<>());
        for (Map.Entry<String, List<Integer>> entry : shuffled.entrySet()) {
            resultsMap.get("Shuffle").add("(" + entry.getKey() + ", " + entry.getValue() + ")");
        }
        System.out.println("Resultado de la fase Shuffle:");
        resultsMap.get("Shuffle").forEach(System.out::println);

        return shuffled;
    }
}
