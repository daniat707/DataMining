import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

public class ReduceNode implements Callable<Map<String, Integer>> {

    private List<String> shuffleFiles;
    private String outputFilePath;
    private boolean induceError;

    public ReduceNode(List<String> shuffleFiles, String outputFilePath, boolean induceError) {
        this.shuffleFiles = shuffleFiles;
        this.outputFilePath = outputFilePath;
        this.induceError = induceError;
    }

    public boolean isError() {
        return induceError;
    }

    @Override
    public Map<String, Integer> call() throws Exception {
        // Simulamos un fallo inducido en el nodo Reduce
        if (induceError) {
            System.out.println("\u001B[31mError inducido en el Nodo Reduce. Fallo en el procesamiento de shuffleFiles.\u001B[0m");
            throw new Exception("Nodo Reduce falló intencionalmente.");
        }

        Map<String, Integer> finalCounts = new TreeMap<>();
        for (String shuffleFile : shuffleFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(shuffleFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // Limpiar la línea y parsear la palabra y los conteos
                    String cleanedLine = line.replaceAll("[^a-zA-Z0-9,\\[\\]]", "");
                    String[] parts = cleanedLine.split(",", 2);
                    String word = parts[0].replace("(", "").trim();

                    String[] counts = parts[1].replace("[", "").replace("]", "").trim().split(",");
                    int sum = 0;
                    for (String count : counts) {
                        if (!count.isEmpty()) {
                            sum += Integer.parseInt(count.trim());
                        }
                    }
                    finalCounts.merge(word, sum, Integer::sum);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        saveReduceOutput(finalCounts, outputFilePath);
        System.out.println("REDUCE Nodo finalizó exitosamente el procesamiento de shuffleFiles.");

        return finalCounts;
    }

    private void saveReduceOutput(Map<String, Integer> finalCounts, String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            for (Map.Entry<String, Integer> entry : finalCounts.entrySet()) {
                writer.write("(" + entry.getKey() + ", " + entry.getValue() + ")\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
