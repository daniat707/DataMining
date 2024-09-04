import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class MapNode implements Callable<Map<String, List<Integer>>> {

    private List<String> chunkFiles;
    private String outputFilePath;

    public MapNode(List<String> chunkFiles, String outputFilePath) {
        this.chunkFiles = chunkFiles;
        this.outputFilePath = outputFilePath;
    }

    @Override
    public Map<String, List<Integer>> call() {
        Map<String, List<Integer>> wordCount = new TreeMap<>();
        for (String chunkFile : chunkFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(chunkFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // Limpiar la línea eliminando todo lo que no sea letras y pasando a minúsculas
                    line = line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase();
                    String[] words = line.split("\\s+");
                    for (String word : words) {
                        if (!word.isEmpty()) {
                            // Cada palabra es independiente, no acumulamos aquí
                            wordCount.computeIfAbsent(word, k -> new ArrayList<>()).add(1);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        saveMapOutput(wordCount, outputFilePath);
        return wordCount;
    }

    private void saveMapOutput(Map<String, List<Integer>> wordCount, String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            for (Map.Entry<String, List<Integer>> entry : wordCount.entrySet()) {
                for (Integer count : entry.getValue()) {
                    writer.write("(" + entry.getKey() + ", " + count + ")\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
