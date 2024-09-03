package mapreduce;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class ReduceNode implements Runnable {

    private String[] mapOutputFiles;

    public ReduceNode(String[] mapOutputFiles) {
        this.mapOutputFiles = mapOutputFiles;
    }

    @Override
    public void run() {
        Map<String, Integer> finalWordCount = new HashMap<>();
        for (String mapOutputFile : mapOutputFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(mapOutputFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    finalWordCount.put(word, finalWordCount.getOrDefault(word, 0) + count);
                }
            } catch (IOException e) {
                throw new RuntimeException("Error processing map output file: " + mapOutputFile, e);
            }
        }
        saveOutput(finalWordCount);
    }

    private void saveOutput(Map<String, Integer> finalWordCount) {
        String outputFile = "reduce_output_" + Thread.currentThread().getId() + ".txt";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            for (Map.Entry<String, Integer> entry : finalWordCount.entrySet()) {
                bw.write(entry.getKey() + " " + entry.getValue());
                bw.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error saving reduce output", e);
        }
    }
}
