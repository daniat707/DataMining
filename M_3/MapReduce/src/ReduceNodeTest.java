import java.io.*;
import java.util.*;

public class ReduceNodeTest {
    public static void main(String[] args) {
        List<String> shuffleFiles = new ArrayList<>();
        
        // Cambia la ruta aquí a la ubicación de tu archivo de texto
        shuffleFiles.add("/Users/alexperez/Desktop/texto.txt"); // Ruta a tu archivo de prueba

        // Crear una instancia de ReduceNode directamente en el mismo archivo
        try {
            Map<String, Integer> result = processShuffleFiles(shuffleFiles);

            // Mostrar los resultados
            for (Map.Entry<String, Integer> entry : result.entrySet()) {
                System.out.println("Palabra: " + entry.getKey() + " - Conteo: " + entry.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<String, Integer> processShuffleFiles(List<String> shuffleFiles) {
        Map<String, Integer> finalCounts = new TreeMap<>();
        for (String shuffleFile : shuffleFiles) {
            System.out.println("Procesando archivo shuffle: " + shuffleFile);
            try (BufferedReader br = new BufferedReader(new FileReader(shuffleFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    System.out.println("Linea leída: " + line);
    
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
    
                    System.out.println("Palabra: " + word + " - Suma: " + sum);
    
                    // Sumar la frecuencia en el map final
                    finalCounts.merge(word, sum, Integer::sum);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return finalCounts;
    }
    
}
