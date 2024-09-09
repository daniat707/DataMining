import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.io.FileWriter;

public class Coordinator {

    private Map<String, List<String>> resultsMap;
    private int chunkSize;
    private String filePath;
    private String outputFilePath;
    private int numChunks; // Número de chunks a generar dinámicamente
    private long startOffset; // Punto de inicio desde donde se debe empezar a leer el archivo

    public Coordinator(int chunkSize, String filePath, String outputFolder, int numChunks, long startOffset) {
        this.resultsMap = new TreeMap<>();
        this.chunkSize = chunkSize;
        this.filePath = filePath;
        this.outputFilePath = "/Users/alexperez/Documents/GitHub/DM1/M_4/MapReduce/src/Files/Chunks/" + outputFolder;
        this.numChunks = numChunks; // Controlar dinámicamente el número de chunks
        this.startOffset = startOffset; // Punto desde donde comenzará a leer el archivo
    }

    public void startProcessing() throws InterruptedException, ExecutionException, IOException {
        deleteDirectory(new File(outputFilePath));
        System.out.println("\u001B[34mInfo: Carpeta 'Chunks' para " + outputFilePath + " borrada. Procesando...\u001B[0m");
        Thread.sleep(2000); // Espera 2 segundos para asegurar que el directorio ha sido borrado
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
        return dir.delete();
    }

    public List<String> split() {
        List<String> chunks = new ArrayList<>();
        File directory = new File(outputFilePath);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(startOffset); // Empezar a leer desde el offset

            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            int chunkCount = 0;

            while ((bytesRead = raf.read(buffer)) != -1 && chunkCount < numChunks) {
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
        int numMapNodes = 4; // Número de nodos Map
        ExecutorService mapPool = Executors.newFixedThreadPool(numMapNodes);
        List<Future<Map<String, List<Integer>>>> mapFutures = new ArrayList<>();

        for (int i = 0; i < chunks.size(); i++) {
            List<String> singleChunk = chunks.subList(i, i + 1); // Procesar un chunk por nodo
            MapNode mapNode = new MapNode(singleChunk, outputFilePath + "map_chunk_" + i + ".txt");
            Future<Map<String, List<Integer>>> future = mapPool.submit(mapNode);
            mapFutures.add(future);
        }

        mapPool.shutdown();
        while (!mapPool.isTerminated()) {}

        System.out.println("Fase Map completada.");
    }

    public void executeShuffle(int mapResultsCount) {
        int numShuffleNodes = 4; // Número de nodos Shuffle
        ExecutorService shufflePool = Executors.newFixedThreadPool(numShuffleNodes);

        List<String> mapFiles = new ArrayList<>();
        for (int i = 0; i < mapResultsCount; i++) { // Solo añadir los archivos generados por este coordinador
            mapFiles.add(outputFilePath + "map_chunk_" + i + ".txt");
        }

        int shuffleSize = (int) Math.ceil(mapFiles.size() / (double) numShuffleNodes);

        List<Future<Map<String, List<Integer>>>> shuffleFutures = new ArrayList<>();
        for (int i = 0; i < numShuffleNodes; i++) {
            int start = i * shuffleSize;
            int end = Math.min(start + shuffleSize, mapFiles.size());
            List<String> mapSubset = mapFiles.subList(start, end); // Agrupar los resultados de map para shuffle
            ShuffleNode shuffleNode = new ShuffleNode(mapSubset, outputFilePath + "shuffle_" + i + ".txt");
            Future<Map<String, List<Integer>>> future = shufflePool.submit(shuffleNode);
            shuffleFutures.add(future);
        }

        shufflePool.shutdown();
        while (!shufflePool.isTerminated()) {}

        System.out.println("Fase Shuffle completada.");
    }

    public void executeReduce() {
        int numReduceNodes = 2; // Número de nodos Reduce
        ExecutorService reducePool = Executors.newFixedThreadPool(numReduceNodes);

        List<String> allShuffleFiles = new ArrayList<>();
        for (int i = 0; i < 4; i++) { // Los 4 archivos shuffle generados
            allShuffleFiles.add(outputFilePath + "shuffle_" + i + ".txt");
        }

        // Dividir los 4 archivos shuffle entre dos nodos reduce
        List<String> firstHalfShuffleFiles = allShuffleFiles.subList(0, 2);
        List<String> secondHalfShuffleFiles = allShuffleFiles.subList(2, 4);

        // Reducer 1 trabaja con los primeros 2 archivos shuffle
        Future<Map<String, Integer>> reduceFuture1 = reducePool.submit(new ReduceNode(firstHalfShuffleFiles, outputFilePath + "reduce_1.txt"));
        // Reducer 2 trabaja con los últimos 2 archivos shuffle
        Future<Map<String, Integer>> reduceFuture2 = reducePool.submit(new ReduceNode(secondHalfShuffleFiles, outputFilePath + "reduce_2.txt"));

        reducePool.shutdown();
        while (!reducePool.isTerminated()) {}

        try {
            Map<String, Integer> finalResults1 = reduceFuture1.get();
            Map<String, Integer> finalResults2 = reduceFuture2.get();

            // Guardar los resultados finales
            try (FileWriter writer = new FileWriter(outputFilePath + "final_reduce_1.txt")) {
                for (Map.Entry<String, Integer> entry : finalResults1.entrySet()) {
                    writer.write("(" + entry.getKey() + ", " + entry.getValue() + ")\n");
                }
            }

            try (FileWriter writer = new FileWriter(outputFilePath + "final_reduce_2.txt")) {
                for (Map.Entry<String, Integer> entry : finalResults2.entrySet()) {
                    writer.write("(" + entry.getKey() + ", " + entry.getValue() + ")\n");
                }
            }

        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("Fase Reduce completada. Resultados finales guardados.");
    }
}
