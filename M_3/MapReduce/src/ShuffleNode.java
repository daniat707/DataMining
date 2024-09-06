import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class ShuffleNode implements Callable<Map<String, List<Integer>>> {

    private List<String> mapFiles;
    private String outputFilePath;

    public ShuffleNode(List<String> mapFiles, String outputFilePath) {
        this.mapFiles = mapFiles;
        this.outputFilePath = outputFilePath;
    }

    @Override
    public Map<String, List<Integer>> call() {
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
