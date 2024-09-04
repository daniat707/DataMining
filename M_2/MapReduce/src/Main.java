import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        // Simulación de datos de entrada
        // List<String> input = Arrays.asList("hola mundo", "hola de nuevo mundo");
        String ruta = "/Users/alexperez/Documents/GitHub/DM1/input.txt";

        //List<String> input = readFile(ruta);
        // Crear y ejecutar el coordinador de MapReduce
        Coordinator coordinator = new Coordinator(32 * 1000 * 1000, ruta);
        coordinator.startProcessing();
        coordinator.split();
        //coordinator.execute();
    }

    /*public static List<String> readFile(String ruta) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(ruta))) {
            String line;
            while ((line = br.readLine()) != null) {
                // Limpiar la línea eliminando todo lo que no sea letras o números y pasando a minúsculas
                line = line.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
                if (!line.isEmpty()) {
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }
    */
}



