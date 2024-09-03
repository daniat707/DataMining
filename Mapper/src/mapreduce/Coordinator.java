package mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Coordinator {

    private List<String> chunkFiles;

    public Coordinator(List<String> chunkFiles) {
        this.chunkFiles = chunkFiles;
    }

    public void execute() {
        int numMapNodes = 4; // Número de nodos Map
        int numReduceNodes = 2; // Número de nodos Reduce

        // Crear un pool de hilos para los nodos Map
        ExecutorService mapPool = Executors.newFixedThreadPool(numMapNodes);
        List<Future<Map<String, Integer>>> mapResults = new ArrayList<>();

        // Distribuir los chunks entre los MapNodes
        int chunksPerNode = chunkFiles.size() / numMapNodes;
        int remainder = chunkFiles.size() % numMapNodes;
        int start = 0;

        for (int i = 0; i < numMapNodes; i++) {
            int end = start + chunksPerNode + (i < remainder ? 1 : 0); // Distribución equitativa
            List<String> assignedChunks = chunkFiles.subList(start, end);
            for (String chunkFile : assignedChunks) {
                MapNode mapNode = new MapNode(chunkFile);
                Future<Map<String, Integer>> future = mapPool.submit(mapNode);
                mapResults.add(future);
            }
            start = end;
        }

        mapPool.shutdown();
        while (!mapPool.isTerminated()) {}

        // Asignar salidas de Map a nodos Reduce
        ExecutorService reducePool = Executors.newFixedThreadPool(numReduceNodes);

        // Reducer 1 trabajará con las salidas de los primeros dos MapNodes
        String[] reduceInput1 = mapResults.subList(0, mapResults.size() / 2).stream()
            .map(future -> getMapOutputFileName(future))
            .toArray(String[]::new);

        // Reducer 2 trabajará con las salidas de los últimos dos MapNodes
        String[] reduceInput2 = mapResults.subList(mapResults.size() / 2, mapResults.size()).stream()
            .map(future -> getMapOutputFileName(future))
            .toArray(String[]::new);

        reducePool.submit(new ReduceNode(reduceInput1));
        reducePool.submit(new ReduceNode(reduceInput2));

        reducePool.shutdown();
        while (!reducePool.isTerminated()) {}

        System.out.println("MapReduce completado.");
    }

    private String getMapOutputFileName(Future<Map<String, Integer>> future) {
        try {
            // Solo devolver el nombre del archivo de salida sin volver a escribirlo
            return "chunk_" + future.hashCode() + "_map_output.txt";
        } catch (Exception e) {
            throw new RuntimeException("Error retrieving output file name", e);
        }
    }
}
