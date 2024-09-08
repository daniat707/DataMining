import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Coordinator {

    private Map<String, List<String>> resultsMap;
    private int chunkSize;
    private String filePath;
    private String outputFilePath;
    private int numChunks;
    private long startOffset;
    private int startChunkIndex;

    private boolean induceCoordinatorError;
    private boolean induceMapError;
    private boolean induceShuffleError;
    private boolean induceReduceError;

    // Constructor con `startChunkIndex` para manejar la numeración continua de los chunks
    public Coordinator(int chunkSize, String filePath, String outputFolder, int numChunks, long startOffset, int startChunkIndex, boolean induceCoordinatorError, boolean induceMapError, boolean induceShuffleError, boolean induceReduceError) {
        this.resultsMap = new TreeMap<>();
        this.chunkSize = chunkSize;
        this.filePath = filePath;
        this.outputFilePath = "/Users/alexperez/Documents/GitHub/DM1/M_F/MapReduce/src/Files/Chunks/" + outputFolder;
        this.numChunks = numChunks;
        this.startOffset = startOffset;
        this.startChunkIndex = startChunkIndex;
        this.induceCoordinatorError = induceCoordinatorError;
        this.induceMapError = induceMapError;
        this.induceShuffleError = induceShuffleError;
        this.induceReduceError = induceReduceError;
    }

    public boolean isCoordinatorError() {
        return induceCoordinatorError;
    }

    public void startProcessing() throws InterruptedException, ExecutionException, IOException {
        deleteDirectory(new File(outputFilePath));
        System.out.println("\u001B[34mInfo: Carpeta 'Chunks' para " + outputFilePath + " borrada. Procesando...\u001B[0m");
        Thread.sleep(2000);
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

    // Método para dividir el archivo en chunks y asegurar la numeración continua
    public List<String> split() {
        List<String> chunks = new ArrayList<>();
        File directory = new File(outputFilePath);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            raf.seek(startOffset); // Empieza desde el offset especificado

            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            int chunkCount = startChunkIndex; // Empezamos la numeración desde `startChunkIndex`

            while ((bytesRead = raf.read(buffer)) != -1 && chunkCount < startChunkIndex + numChunks) {
                String chunkFileName = outputFilePath + "chunk_" + chunkCount + ".txt"; // Usa `chunkCount` para numerar los chunks
                try (FileOutputStream fos = new FileOutputStream(chunkFileName)) {
                    fos.write(buffer, 0, bytesRead);
                    chunks.add(chunkFileName);
                    chunkCount++; // Incrementa el contador de chunks para continuar la numeración
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return chunks;
    }

    // Método que ejecuta el proceso Map y reasigna los chunks fallidos
    public void executeMap(List<String> chunks) {
        int numMapNodes = 4; 
        ExecutorService mapPool = Executors.newFixedThreadPool(numMapNodes);
        List<Future<Map<String, List<Integer>>>> mapFutures = new ArrayList<>();

        Set<Integer> failedChunks = new HashSet<>(); // Chunks que fallaron y necesitan ser reasignados

        for (int i = 0; i < chunks.size(); i++) {
            final int chunkIndex = i + startChunkIndex;  // Aseguramos que la numeración continúe
            List<String> singleChunk = chunks.subList(i, i + 1);
            
            // Aquí se numera correctamente el archivo map_chunk_ con la secuencia correcta
            MapNode mapNode = new MapNode(singleChunk, outputFilePath + "map_chunk_" + chunkIndex + ".txt", induceMapError && i == 0, false); 
            
            Future<Map<String, List<Integer>>> future = mapPool.submit(() -> {
                if (mapNode.isError()) {
                    System.out.println("\u001B[31mError inducido en Nodo Map para chunk: " + chunkIndex + "\u001B[0m");
                    failedChunks.add(chunkIndex); // Guardamos el chunk fallido
                    return null; // Simulamos un fallo
                } else {
                    System.out.println("MAP Nodo procesando chunk: " + chunkIndex);
                    return mapNode.call();
                }
            });
            mapFutures.add(future);
        }

        mapPool.shutdown();
        while (!mapPool.isTerminated()) {}

        // Intentar reasignar chunks fallidos
        if (!failedChunks.isEmpty()) {
            System.out.println("\u001B[33mReasignando chunks fallidos...\u001B[0m");
            ExecutorService reassignmentPool = Executors.newFixedThreadPool(numMapNodes); // Nuevo pool para reasignar
            for (int chunkIndex : failedChunks) {
                Future<Map<String, List<Integer>>> reassignedFuture = reassignmentPool.submit(() -> {
                    MapNode mapNode = new MapNode(chunks.subList(chunkIndex - startChunkIndex, chunkIndex - startChunkIndex + 1), outputFilePath + "map_chunk_" + chunkIndex + ".txt", false, true);
                    System.out.println("MAP Nodo reasignado procesando chunk: " + chunkIndex);
                    return mapNode.call();
                });
                mapFutures.add(reassignedFuture);
            }

            reassignmentPool.shutdown();
            while (!reassignmentPool.isTerminated()) {}
        }

        System.out.println("Fase Map completada.");
    }

    // Método que ejecuta el proceso Shuffle y reasigna las tareas fallidas
    public void executeShuffle(int mapResultsCount) {
        int numShuffleNodes = 4; 
        ExecutorService shufflePool = Executors.newFixedThreadPool(numShuffleNodes);

        List<String> mapFiles = new ArrayList<>();
        Set<Integer> failedShuffles = new HashSet<>(); // Subsets que fallaron y necesitan ser reasignados

        // Aquí ajustamos el índice de los archivos de map_chunk_
        for (int i = 0; i < mapResultsCount; i++) {
            int chunkIndex = i + startChunkIndex;  // Ajustamos la numeración para que comience correctamente
            mapFiles.add(outputFilePath + "map_chunk_" + chunkIndex + ".txt");
        }

        int shuffleSize = (int) Math.ceil(mapFiles.size() / (double) numShuffleNodes);

        List<Future<Map<String, List<Integer>>>> shuffleFutures = new ArrayList<>();
        for (int i = 0; i < numShuffleNodes; i++) {
            final int shuffleIndex = i;
            int start = i * shuffleSize;
            int end = Math.min(start + shuffleSize, mapFiles.size());
            List<String> mapSubset = mapFiles.subList(start, end);
            ShuffleNode shuffleNode = new ShuffleNode(mapSubset, outputFilePath + "shuffle_" + i + ".txt", induceShuffleError && i == 0, false); // Induce error en el primer nodo si se pidió
            Future<Map<String, List<Integer>>> future = shufflePool.submit(() -> {
                if (shuffleNode.isError()) {
                    System.out.println("\u001B[31mError inducido en Nodo Shuffle para subset: " + shuffleIndex + "\u001B[0m");
                    failedShuffles.add(shuffleIndex); // Guardamos los subsets fallidos
                    return null; // Simulamos un fallo
                } else {
                    System.out.println("SHUFFLE Nodo procesando subset: " + shuffleIndex);
                    return shuffleNode.call();
                }
            });
            shuffleFutures.add(future);
        }

        shufflePool.shutdown();
        while (!shufflePool.isTerminated()) {}

        // Intentar reasignar los subsets fallidos
        if (!failedShuffles.isEmpty()) {
            System.out.println("\u001B[33mReasignando subsets fallidos en Shuffle...\u001B[0m");
            ExecutorService reassignmentPool = Executors.newFixedThreadPool(numShuffleNodes); // Nuevo pool para reasignar
            for (int shuffleIndex : failedShuffles) {
                Future<Map<String, List<Integer>>> reassignedFuture = reassignmentPool.submit(() -> {
                    List<String> mapSubset = mapFiles.subList(shuffleIndex * shuffleSize, Math.min((shuffleIndex + 1) * shuffleSize, mapFiles.size()));
                    ShuffleNode shuffleNode = new ShuffleNode(mapSubset, outputFilePath + "shuffle_" + shuffleIndex + ".txt", false, true);
                    System.out.println("SHUFFLE Nodo reasignado procesando subset: " + shuffleIndex);
                    return shuffleNode.call();
                });
                shuffleFutures.add(reassignedFuture);
            }

            reassignmentPool.shutdown();
            while (!reassignmentPool.isTerminated()) {}
        }

        System.out.println("Fase Shuffle completada.");
    }

    // Método para ejecutar la fase Reduce (se mantiene igual)
    public void executeReduce() {
        int numReduceNodes = 2; 
        ExecutorService reducePool = Executors.newFixedThreadPool(numReduceNodes);

        List<String> allShuffleFiles = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            allShuffleFiles.add(outputFilePath + "shuffle_" + i + ".txt");
        }

        List<String> firstHalfShuffleFiles = allShuffleFiles.subList(0, 2);
        List<String> secondHalfShuffleFiles = allShuffleFiles.subList(2, 4);

        Future<Map<String, Integer>> reduceFuture1 = reducePool.submit(new ReduceNode(firstHalfShuffleFiles, outputFilePath + "reduce_1.txt", induceReduceError && 0 == 0)); // Induce error en el primer nodo si se pidió
        Future<Map<String, Integer>> reduceFuture2 = reducePool.submit(new ReduceNode(secondHalfShuffleFiles, outputFilePath + "reduce_2.txt", induceReduceError && 1 == 0));

        reducePool.shutdown();
        while (!reducePool.isTerminated()) {}

        try {
            System.out.println("Fase Reduce completada.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
