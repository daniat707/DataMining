package mapreduce;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class MapNode implements Callable<Map<String, Integer>> {

    private String chunkFile;

    public MapNode(String chunkFile) {
        this.chunkFile = chunkFile;
    }

    @Override
    public Map<String, Integer> call() throws Exception {
        Map<String, Integer> wordCount = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(chunkFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = line.split("\\s+");
                for (String word : words) {
                    word = word.replaceAll("[^a-zA-Z]", ""); // Mantén las mayúsculas
                    if (!word.isEmpty()) {
                        wordCount.put(word, wordCount.getOrDefault(word, 0) + 1);
                    }
                }
            }
            saveOutput(wordCount);
        } catch (IOException e) {
            throw new RuntimeException("Error processing chunk: " + chunkFile, e);
        }
        return wordCount;
    }

    private void saveOutput(Map<String, Integer> wordCount) {
        String outputFile = chunkFile.replace(".txt", "_map_output.txt");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
                bw.write(entry.getKey() + " " + entry.getValue());
                bw.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error saving output for chunk: " + chunkFile, e);
        }
    }
}
