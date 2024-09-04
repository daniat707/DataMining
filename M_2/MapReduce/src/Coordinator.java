import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Coordinator {

    private List<String> inputData;
    private Map<String, List<String>> resultsMap;
    private int chunkSize;
    private String filePath;
    private String outputFilePath = "/Users/alexperez/Downloads/M_2/MapReduce/src/Files/Chunks/";

    public Coordinator(int chunkSize, String filePath) {
        this.resultsMap = new TreeMap<>();
        this.chunkSize = 32 * 1000 * 1000;
        this.filePath = filePath;
    }

    public void startProcessing() throws InterruptedException, ExecutionException, IOException {
        deleteDirectory(new File(outputFilePath));
        System.out.println("\u001B[34mInfo: Carpeta 'Chunks' borrada. Procesando...\u001B[0m");
        Thread.sleep(5000); // Espera 5 segundos para asegurar que el directorio ha sido borrado
    }

    private boolean deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!deleteDirectory(file)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public List<String> split() {
        List<String> chunks = new ArrayList<>();
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filePath))) {
            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            int chunkCount = 0;

            while ((bytesRead = bis.read(buffer)) != -1) {
                String chunkFileName = outputFilePath + "chunk_" + chunkCount + ".txt";
                try (FileOutputStream fos = new FileOutputStream(chunkFileName)) {
                    fos.write(buffer, 0, bytesRead);
                    chunks.add(chunkFileName);
                    chunkCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return chunks;
    }

    public void executeMap(List<String> chunks) {
        int numMapNodes = 4; // Número de hilos para map
        ExecutorService mapPool = Executors.newFixedThreadPool(numMapNodes);
        List<Future<Map<String, List<Integer>>>> mapFutures = new ArrayList<>();
    
        int chunksPerNode = 10;
        for (int i = 0; i < numMapNodes; i++) {
            int start = i * chunksPerNode;
            int end = Math.min(start + chunksPerNode, chunks.size());
    
            List<String> chunkSubset = chunks.subList(start, end);
            MapNode mapNode = new MapNode(chunkSubset, outputFilePath + "map_" + i + ".txt");
            Future<Map<String, List<Integer>>> future = mapPool.submit(mapNode);
            mapFutures.add(future);
        }
    
        mapPool.shutdown();
        while (!mapPool.isTerminated()) {}
    
        System.out.println("Fase Map completada.");
    }

    public void executeShuffle() {
        int numShuffleNodes = 4; // Número de hilos para shuffle
        ExecutorService shufflePool = Executors.newFixedThreadPool(numShuffleNodes);
    
        List<String> mapFiles = new ArrayList<>();
        for (int i = 0; i < numShuffleNodes; i++) {
            mapFiles.add(outputFilePath + "map_" + i + ".txt");
        }
    
        List<Future<Map<String, List<Integer>>>> shuffleFutures = new ArrayList<>();
        for (int i = 0; i < numShuffleNodes; i++) {
            // Cada nodo shuffle procesará uno de los archivos generados por los mappers
            ShuffleNode shuffleNode = new ShuffleNode(mapFiles.subList(i, i + 1), outputFilePath + "shuffle_" + i + ".txt");
            Future<Map<String, List<Integer>>> future = shufflePool.submit(shuffleNode);
            shuffleFutures.add(future);
        }
    
        shufflePool.shutdown();
        while (!shufflePool.isTerminated()) {}
    
        // Verificar los resultados de la fase de shuffle
        System.out.println("Fase Shuffle completada.");
    }
    

    public void executeReduce() {
        int numReduceNodes = 2; // Número de hilos para reduce
        ExecutorService reducePool = Executors.newFixedThreadPool(numReduceNodes);
    
        List<String> shuffleFilesForReducer1 = List.of(outputFilePath + "shuffle_0.txt", outputFilePath + "shuffle_1.txt");
        List<String> shuffleFilesForReducer2 = List.of(outputFilePath + "shuffle_2.txt", outputFilePath + "shuffle_3.txt");
    
        // Reducers procesan los archivos shuffle y generan reduce_1.txt y reduce_2.txt
        Future<Map<String, Integer>> reduceFuture1 = reducePool.submit(new ReduceNode(shuffleFilesForReducer1, outputFilePath + "reduce_1.txt"));
        Future<Map<String, Integer>> reduceFuture2 = reducePool.submit(new ReduceNode(shuffleFilesForReducer2, outputFilePath + "reduce_2.txt"));
    
        reducePool.shutdown();
        while (!reducePool.isTerminated()) {}
    
        // Una vez que se completa la fase reduce, combinamos los resultados de reduce_1.txt y reduce_2.txt
        try {
            Map<String, Integer> finalResults = new TreeMap<>();
            
            // Obtener los resultados de reduceFuture1
            Map<String, Integer> result1 = reduceFuture1.get();
            for (Map.Entry<String, Integer> entry : result1.entrySet()) {
                finalResults.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
    
            // Obtener los resultados de reduceFuture2
            Map<String, Integer> result2 = reduceFuture2.get();
            for (Map.Entry<String, Integer> entry : result2.entrySet()) {
                finalResults.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
    
            // Guardar los resultados finales en un archivo único
            try (FileWriter writer = new FileWriter(outputFilePath + "final_reduce.txt")) {
                for (Map.Entry<String, Integer> entry : finalResults.entrySet()) {
                    writer.write("(" + entry.getKey() + ", " + entry.getValue() + ")\n");
                }
            }
    
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    
        System.out.println("Fase Reduce completada. Resultados finales guardados en final_reduce.txt");
    }
    

}