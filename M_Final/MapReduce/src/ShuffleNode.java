import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

public class ShuffleNode implements Callable<Map<String, List<Integer>>> {

    private List<String> mapFiles;
    private String outputFilePath;
    private String coordinatorId;
    private int nodeId;
    private boolean induceError;
    private boolean reassigned;

    public ShuffleNode(List<String> mapFiles, String outputFilePath, String coordinatorId, int nodeId, boolean induceError, boolean reassigned) {
        this.mapFiles = mapFiles;
        this.outputFilePath = outputFilePath;
        this.coordinatorId = coordinatorId;
        this.nodeId = nodeId;
        this.induceError = induceError;
        this.reassigned = reassigned;

    }

    public boolean isError() {
        return induceError;
    }

    @Override
    public Map<String, List<Integer>> call() throws Exception {
        // Simular un fallo inducido en el nodo Shuffle
        if (induceError) {
            System.out.println("\u001B[31mError inducido en el Nodo Shuffle " + nodeId + " del " + coordinatorId + ". Fallo en el procesamiento de mapFiles.\u001B[0m");
            throw new Exception("Nodo Shuffle " + nodeId + " del " + coordinatorId + " falló intencionalmente.");
        }

        if (reassigned) {
            System.out.println("\u001B[33mNodo Shuffle " + nodeId + " del " + coordinatorId + " reasignado. Procesando mapFiles...\u001B[0m");
        }

        Map<String, List<Integer>> shuffledData = new TreeMap<>();
        for (String mapFile : mapFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(mapFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // Separar la palabra y el conteo desde el archivo
                    String cleanedLine = line.replaceAll("[^a-zA-Z0-9,()]", "");
                    String[] parts = cleanedLine.split(",");
                    String word = parts[0].replace("(", "");
                    int count = Integer.parseInt(parts[1].replace(")", "").trim());
                    shuffledData.computeIfAbsent(word, k -> new ArrayList<>()).add(count);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("SHUFFLE Nodo " + nodeId + " del " + coordinatorId + " finalizó exitosamente el procesamiento de " + new File(mapFile).getName());
        
        }
        saveShuffleOutput(shuffledData, outputFilePath);

        return shuffledData;
    }

    private void saveShuffleOutput(Map<String, List<Integer>> shuffledData, String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            for (Map.Entry<String, List<Integer>> entry : shuffledData.entrySet()) {
                writer.write("(" + entry.getKey() + ", " + entry.getValue().toString() + ")\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
