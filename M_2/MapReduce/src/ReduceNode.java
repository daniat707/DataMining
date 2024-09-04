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
            try (BufferedReader br = new BufferedReader(new FileReader(shuffleFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // Limpiar la línea y parsear la palabra y los conteos
                    String cleanedLine = line.replaceAll("[^a-zA-Z0-9,()\\[\\]]", "");
                    String[] parts = cleanedLine.split(",");
                    String word = parts[0].replace("(", "");
                    String[] counts = parts[1].replace(")", "").replace("[", "").replace("]", "").split(" ");

                    int sum = 0;
                    for (String count : counts) {
                        sum += Integer.parseInt(count.trim());
                    }

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
