import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

public class ShuffleNode implements Callable<Map<String, List<Integer>>> {

    private List<String> mapFiles;
    private String outputFilePath;
    private boolean induceError;
    private boolean reassigned;

    public ShuffleNode(List<String> mapFiles, String outputFilePath, boolean induceError, boolean reassigned) {
        this.mapFiles = mapFiles;
        this.outputFilePath = outputFilePath;
        this.induceError = induceError;
        this.reassigned = reassigned;

    }

    public boolean isError() {
        return induceError;
    }

    @Override
    public Map<String, List<Integer>> call() throws Exception {
        // Simulamos un fallo inducido en el nodo Shuffle
        if (induceError) {
            System.out.println("\u001B[31mError inducido en el Nodo Shuffle. Fallo en el procesamiento de mapFiles.\u001B[0m");
            throw new Exception("Nodo Shuffle falló intencionalmente.");
        }

        if (reassigned) {
            System.out.println("\u001B[33mNodo Shuffle reasignado. Procesando mapFiles...\u001B[0m");
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
        }
        saveShuffleOutput(shuffledData, outputFilePath);
        System.out.println("SHUFFLE Nodo finalizó exitosamente el procesamiento de mapFiles.");

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
