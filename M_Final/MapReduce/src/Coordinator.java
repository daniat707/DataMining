import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Coordinator {

    private String coordinatorId; // Identificador para saber si es MapReduce 1 o 2
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

    // Mantiene nodos activos
    private List<Boolean> activeNodesMap;
    private List<Boolean> activeNodesShuffle;

    // Constructor con `startChunkIndex` para manejar la numeración continua de los chunks
    public Coordinator(String coordinatorId, int chunkSize, String filePath, String outputFolder, int numChunks, long startOffset, int startChunkIndex, boolean induceCoordinatorError, boolean induceMapError, boolean induceShuffleError, boolean induceReduceError) {
        this.coordinatorId = coordinatorId;
        this.resultsMap = new TreeMap<>();
        this.chunkSize = chunkSize;
        this.filePath = filePath;
        this.outputFilePath = "/Users/alexperez/Documents/GitHub/DM1/M_Final/MapReduce/src/Files/Chunks/" + outputFolder;
        this.numChunks = numChunks;
        this.startOffset = startOffset;
        this.startChunkIndex = startChunkIndex;
        this.induceCoordinatorError = induceCoordinatorError;
        this.induceMapError = induceMapError;
        this.induceShuffleError = induceShuffleError;
        this.induceReduceError = induceReduceError;

        // Inicializamos los nodos como activos
        this.activeNodesMap = new ArrayList<>(Collections.nCopies(4, true)); // 4 nodos de Map
        this.activeNodesShuffle = new ArrayList<>(Collections.nCopies(4, true)); // 4 nodos de Shuffle
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
        
            final int nodeId = i % numMapNodes;  // Identificador del nodo Map
        
            MapNode mapNode = new MapNode(singleChunk, outputFilePath + "map_chunk_" + chunkIndex + ".txt", coordinatorId, nodeId, induceMapError && i == 0, false);
        
            Future<Map<String, List<Integer>>> future = mapPool.submit(() -> {
                if (mapNode.isError()) {
                    System.out.println("\u001B[31mError inducido en Nodo Map " + nodeId + " del " + coordinatorId + " para chunk_" + chunkIndex + "\u001B[0m");

                    // Apagamos el nodo que falló
                    activeNodesMap.set(nodeId, false);

                    // Guardamos el chunk que falló para ser reasignado
                    failedChunks.add(chunkIndex);

                    // Reasignamos el chunk fallido inmediatamente
                    System.out.println("\u001B[33mReasignando chunk_" + chunkIndex + " a otro nodo...\u001B[0m");
                    reassignFailedChunkMap(chunkIndex, chunks);

                    return null; // El nodo Map no sigue procesando
                } else {
                    System.out.println("MAP Nodo " + nodeId + " del " + coordinatorId + " procesando chunk_" + chunkIndex);
                    return mapNode.call();
                }
            });
        
            mapFutures.add(future);
        }

        mapPool.shutdown();
        while (!mapPool.isTerminated()) {}

        System.out.println("Fase Map completada para el " + coordinatorId + ".");
    }

    // Función para reasignar los chunks fallidos inmediatamente a nodos activos (Map)
    private void reassignFailedChunkMap(int chunkIndex, List<String> chunks) {
        int numMapNodes = 4;
        ExecutorService reassignmentPool = Executors.newFixedThreadPool(numMapNodes); // Nuevo pool para reasignar

        Future<Map<String, List<Integer>>> reassignedFuture = reassignmentPool.submit(() -> {
            for (int nodeId = 0; nodeId < numMapNodes; nodeId++) {
                if (activeNodesMap.get(nodeId)) {  // Asegurarse de que el nodo esté activo
                    MapNode mapNode = new MapNode(chunks.subList(chunkIndex - startChunkIndex, chunkIndex - startChunkIndex + 1), outputFilePath + "map_chunk_" + chunkIndex + ".txt", coordinatorId, nodeId, false, true);
                    System.out.println("MAP Nodo " + nodeId + " del " + coordinatorId + " reasignado procesando chunk_" + chunkIndex);
                    return mapNode.call();
                }
            }
            return null;  // Si no hay nodos activos (caso improbable)
        });

        reassignmentPool.shutdown();
        while (!reassignmentPool.isTerminated()) {}
    }

    // Método que ejecuta el proceso Shuffle y reasigna los subsets fallidos
    public void executeShuffle(int mapResultsCount) {
        int numShuffleNodes = 4;
        ExecutorService shufflePool = Executors.newFixedThreadPool(numShuffleNodes);

        List<String> mapFiles = new ArrayList<>();
        Set<Integer> failedShuffles = new HashSet<>(); // Subsets que fallaron y necesitan ser reasignados

        for (int i = 0; i < mapResultsCount; i++) {
            int chunkIndex = i + startChunkIndex;  // Ajustamos la numeración para que comience correctamente
            mapFiles.add(outputFilePath + "map_chunk_" + chunkIndex + ".txt");
        }

        int shuffleSize = (int) Math.ceil(mapFiles.size() / (double) numShuffleNodes);

        List<Future<Map<String, List<Integer>>>> shuffleFutures = new ArrayList<>();
        for (int i = 0; i < numShuffleNodes; i++) {
            final int shuffleIndex = i;  // Creamos una variable final para usar en el lambda
            int start = i * shuffleSize;
            int end = Math.min(start + shuffleSize, mapFiles.size());
            List<String> mapSubset = mapFiles.subList(start, end);

            final int nodeId = i % numShuffleNodes;

            ShuffleNode shuffleNode = new ShuffleNode(mapSubset, outputFilePath + "shuffle_" + i + ".txt", coordinatorId, nodeId, induceShuffleError && i == 0, false);
            
            Future<Map<String, List<Integer>>> future = shufflePool.submit(() -> {
                if (shuffleNode.isError()) {
                    System.out.println("\u001B[31mError inducido en Nodo Shuffle " + nodeId + " del " + coordinatorId + " para subset: " + shuffleIndex + "\u001B[0m");

                    // Apagamos el nodo que falló
                    activeNodesShuffle.set(shuffleIndex % numShuffleNodes, false);

                    // Guardamos el subset fallido para ser reasignado
                    failedShuffles.add(shuffleIndex);

                    // Reasignamos el subset fallido inmediatamente
                    System.out.println("\u001B[33mReasignando subset " + shuffleIndex + " a otro nodo...\u001B[0m");
                    reassignFailedSubsetShuffle(shuffleIndex, mapFiles);

                    return null; // El nodo Shuffle no sigue procesando
                } else {
                    System.out.println("SHUFFLE Nodo " + nodeId + " del " + coordinatorId + " procesando subset: " + shuffleIndex);
                    return shuffleNode.call();
                }
            });
        
            shuffleFutures.add(future);
        }

        shufflePool.shutdown();
        while (!shufflePool.isTerminated()) {}

        System.out.println("Fase Shuffle completada para " + coordinatorId + ".");
    }

    // Función para reasignar los subsets fallidos inmediatamente a nodos activos (Shuffle)
    private void reassignFailedSubsetShuffle(int subsetIndex, List<String> mapFiles) {
        int numShuffleNodes = 4;
        ExecutorService reassignmentPool = Executors.newFixedThreadPool(numShuffleNodes); // Nuevo pool para reasignar

        Future<Map<String, List<Integer>>> reassignedFuture = reassignmentPool.submit(() -> {
            for (int nodeId = 0; nodeId < numShuffleNodes; nodeId++) {
                if (activeNodesShuffle.get(nodeId)) {  // Asegurarse de que el nodo esté activo
                    List<String> mapSubset = mapFiles.subList(subsetIndex * mapFiles.size() / numShuffleNodes, Math.min((subsetIndex + 1) * mapFiles.size() / numShuffleNodes, mapFiles.size()));
                    ShuffleNode shuffleNode = new ShuffleNode(mapSubset, outputFilePath + "shuffle_" + subsetIndex + ".txt", coordinatorId, nodeId, false, true);
                    System.out.println("SHUFFLE Nodo " + nodeId + " del " + coordinatorId + " reasignado procesando subset: " + subsetIndex);
                    return shuffleNode.call();
                }
            }
            return null;  // Si no hay nodos activos (caso improbable)
        });

        reassignmentPool.shutdown();
        while (!reassignmentPool.isTerminated()) {}
    }

    // Método para ejecutar la fase Reduce
    public void executeReduce() {
        int numReduceNodes = 2;
        ExecutorService reducePool = Executors.newFixedThreadPool(numReduceNodes);

        List<String> allShuffleFiles = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            allShuffleFiles.add(outputFilePath + "shuffle_" + i + ".txt");
        }

        List<String> firstHalfShuffleFiles = allShuffleFiles.subList(0, 2);
        List<String> secondHalfShuffleFiles = allShuffleFiles.subList(2, 4);

        Future<Map<String, Integer>> reduceFuture1 = reducePool.submit(new ReduceNode(firstHalfShuffleFiles, outputFilePath + "reduce_1.txt", coordinatorId, 1, induceReduceError)); 
        Future<Map<String, Integer>> reduceFuture2 = reducePool.submit(new ReduceNode(secondHalfShuffleFiles, outputFilePath + "reduce_2.txt", coordinatorId, 2, induceReduceError));

        try {
            // Reinicio para el Nodo Reduce 1 si falló
            if (reduceFuture1.get() == null) {
                System.out.println("Nodo Reduce 1 del " + coordinatorId + " falló. Reiniciando...");
                Thread.sleep(5000); // Simulamos el tiempo de reinicio
                reduceFuture1 = reducePool.submit(new ReduceNode(firstHalfShuffleFiles, outputFilePath + "reduce_1.txt", coordinatorId, 1, false));
                reduceFuture1.get(); // Reintentar el procesamiento de los mismos subsets
            }

            // Reinicio para el Nodo Reduce 2 si falló
            if (reduceFuture2.get() == null) {
                System.out.println("Nodo Reduce 2 del " + coordinatorId + " falló. Reiniciando...");
                Thread.sleep(5000); // Simulamos el tiempo de reinicio
                reduceFuture2 = reducePool.submit(new ReduceNode(secondHalfShuffleFiles, outputFilePath + "reduce_2.txt", coordinatorId, 2, false));
                reduceFuture2.get(); // Reintentar el procesamiento de los mismos subsets
            }

            System.out.println("Fase Reduce completada para " + coordinatorId + ".");
            
        } catch (Exception e) {
            e.printStackTrace();

            // Aquí agregamos un segundo try-catch dentro del `catch` para volver a intentar
            try {
                System.out.println("Intentando reiniciar los nodos Reduce nuevamente después del fallo...");
                Future<Map<String, Integer>> reduceFutureRetry1 = reducePool.submit(new ReduceNode(firstHalfShuffleFiles, outputFilePath + "reduce_1.txt", coordinatorId, 1, false));
                Future<Map<String, Integer>> reduceFutureRetry2 = reducePool.submit(new ReduceNode(secondHalfShuffleFiles, outputFilePath + "reduce_2.txt", coordinatorId, 2, false));
                
                reduceFutureRetry1.get(); // Volvemos a intentar procesar el nodo 1
                reduceFutureRetry2.get(); // Volvemos a intentar procesar el nodo 2

                System.out.println("Fase Reduce reiniciada exitosamente.");
            } catch (Exception retryException) {
                retryException.printStackTrace();
                System.out.println("Nodo Reduce falló nuevamente en el segundo intento.");
            }

        } finally {
            reducePool.shutdown();
            while (!reducePool.isTerminated()) {}
        }
    }
}
