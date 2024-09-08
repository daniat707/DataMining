import java.io.*;
import java.util.*;

public class FinalReduceNode {

    private String[] reduceFiles;
    private String finalOutputFilePath;
    private boolean induceError;

    public FinalReduceNode(String[] reduceFiles, String finalOutputFilePath, boolean induceError) {
        this.reduceFiles = reduceFiles;
        this.finalOutputFilePath = finalOutputFilePath;
        this.induceError = induceError;
    }

    public boolean isError() {
        return induceError;
    }

    public void combineReduceResults() throws Exception {
        // Simular un fallo inducido en el nodo Final Reduce
        if (induceError) {
            System.out.println("\u001B[31mError inducido en el Nodo Final Reduce. Fallo en la combinación de resultados.\u001B[0m");
            throw new Exception("Nodo Final Reduce falló intencionalmente.");
        }
    
        System.out.println("Nodo Final Reduce: Combinando resultados de archivos de reducción...");
    
        Map<String, Integer> finalWordCounts = new TreeMap<>();
        for (String reduceFile : reduceFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(reduceFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // Limpiar la línea y parsear la palabra y el conteo
                    String cleanedLine = line.replaceAll("[^a-zA-Z0-9,()]", "");
                    String[] parts = cleanedLine.split(",");
                    String word = parts[0].replace("(", "");
                    int count = Integer.parseInt(parts[1].replace(")", "").trim());
    
                    finalWordCounts.merge(word, count, Integer::sum);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    
        saveFinalOutput(finalWordCounts);
        System.out.println("\u001B[32mNodo Final Reduce: Combinación de resultados completada exitosamente.\u001B[0m");
    }
    
    private void saveFinalOutput(Map<String, Integer> finalWordCounts) {
        try (FileWriter writer = new FileWriter(finalOutputFilePath)) {
            for (Map.Entry<String, Integer> entry : finalWordCounts.entrySet()) {
                writer.write("(" + entry.getKey() + ", " + entry.getValue() + ")\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
