import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class ReduceNode implements Callable<Map<String, Integer>> {

    private List<String> shuffleFiles;
    private String outputFilePath;

    public ReduceNode(List<String> shuffleFiles, String outputFilePath) {
        this.shuffleFiles = shuffleFiles;
        this.outputFilePath = outputFilePath;
    }

    @Override
    public Map<String, Integer> call() {
        Map<String, Integer> finalCounts = new TreeMap<>();
        for (String shuffleFile : shuffleFiles) {
            //System.out.println("Procesando archivo shuffle: " + shuffleFile);
            try (BufferedReader br = new BufferedReader(new FileReader(shuffleFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    //System.out.println("Linea leída: " + line);
    
                    // Limpiar la línea y parsear la palabra y los conteos
                    String cleanedLine = line.replaceAll("[^a-zA-Z0-9,\\[\\]]", "");
                    String[] parts = cleanedLine.split(",", 2); // Dividir solo en la primera coma
                    String word = parts[0].replace("(", "").trim();
                    
                    // Manejar la lista de conteos
                    String[] counts = parts[1].replace("[", "").replace("]", "").trim().split(",");
                    
                    // Sumar los valores de la lista
                    int sum = 0;
                    for (String count : counts) {
                        if (!count.isEmpty()) {
                            sum += Integer.parseInt(count.trim());  // Asegurarse de que sumamos cada '1'
                        }
                    }
    
                    //System.out.println("Palabra: " + word + " - Suma: " + sum);
    
                    // Sumar la frecuencia en el map final
                    finalCounts.merge(word, sum, Integer::sum);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        saveReduceOutput(finalCounts, outputFilePath);
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