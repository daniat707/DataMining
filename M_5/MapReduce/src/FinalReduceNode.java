import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class FinalReduceNode {

    private String[] reduceFiles;
    private String outputFilePath;

    public FinalReduceNode(String[] reduceFiles, String outputFilePath) {
        this.reduceFiles = reduceFiles;
        this.outputFilePath = outputFilePath;
    }

    public void combineReduceResults() {
        Map<String, Integer> finalResults = new TreeMap<>();

        // Leer cada archivo reduce y combinar los resultados
        for (String reduceFile : reduceFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(reduceFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // Parsear la línea (palabra, conteo)
                    String[] parts = line.replaceAll("[()]", "").split(",\\s*");
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);

                    // Sumar los conteos
                    finalResults.merge(word, count, Integer::sum);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Guardar los resultados combinados en un archivo final
        try (FileWriter writer = new FileWriter(outputFilePath)) {
            for (Map.Entry<String, Integer> entry : finalResults.entrySet()) {
                writer.write("(" + entry.getKey() + ", " + entry.getValue() + ")\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Resultados combinados guardados en " + outputFilePath);
    }
}
