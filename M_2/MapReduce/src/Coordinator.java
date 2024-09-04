import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

public class Coordinator {

    private List<String> inputData;
    private Map<String, List<String>> resultsMap;
    private int chunkSize;
    private String filePath = "/Users/alexperez/Documents/GitHub/DM1/input.txt";
    private String outputFilePath = "/Users/alexperez/Documents/GitHub/DM1/M_2/MapReduce/src/Files/Chunks/";
    //List<String> inputData,

    public Coordinator(int chunkSize, String filePath) {
        //this.inputData = inputData;
        this.resultsMap = new TreeMap<>();
        this.chunkSize = 32 * 1000 * 1000;
        this.filePath = filePath;
        // Remove the redundant assignment
    }
    public void startProcessing() throws InterruptedException, ExecutionException, IOException {
        // Borra la carpeta data antes de comenzar el procesamiento
        deleteDirectory(new File("/Users/alexperez/Documents/GitHub/DM1/M_2/MapReduce/src/Files/Chunks/"));
        System.out.println("\u001B[34mInfo: Carpeta 'data' borrada. Por favor espere unos minutos antes de iniciar el procesamiento...\u001B[0m");
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
                // Crear el nombre del archivo del chunk
                String chunkFileName = outputFilePath + "chunk_" + chunkCount + ".txt"; // Usa outputFilePath para guardar los chunks en el directorio deseado
                try (FileOutputStream fos = new FileOutputStream(chunkFileName)) {
                    fos.write(buffer, 0, bytesRead);
                    chunks.add(chunkFileName); // Agregar el nombre del archivo a la lista de chunks
                    chunkCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return chunks;
    }

    public void execute() {
        int numMapNodes = 4; // Número de hilos para map
        int numReduceNodes = 2; // Número de hilos para reduce

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
