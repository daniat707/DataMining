import mapreduce.FileSplitter;
import mapreduce.Coordinator;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        // Se crea una instancia de FileSplitter
        FileSplitter splitter = new FileSplitter("/Users/alexperez/Documents/DataMining/input.txt", 32 * 1000 * 1000);

        // Se llama al método split() para dividir el archivo en chunks
        List<String> chunks = splitter.split();

        // Se imprime los nombres de los archivos generados (chunks)
        for (String chunk : chunks) {
            System.out.println("Chunk creado: " + chunk);
        }

        // Crear el coordinador y ejecutar el MapReduce
        Coordinator coordinator = new Coordinator(chunks);
        coordinator.execute();
    }
}
