import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        String ruta = "/Users/alexperez/Documents/GitHub/DM1/M_5/Input.txt";
        
        // Tamaño de cada chunk
        int chunkSize = 32 * 1000 * 1000;

        // Coordinadores para los dos grupos de chunks
        Coordinator coordinator1 = new Coordinator(chunkSize, ruta, "MapReduce1/", 20, 0); // 20 chunks
        Coordinator coordinator2 = new Coordinator(chunkSize, ruta, "MapReduce2/", 21, chunkSize * 20); // 21 chunks

        // Procesar en paralelo
        Thread process1 = new Thread(() -> {
            try {
                executeCoordinator(coordinator1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread process2 = new Thread(() -> {
            try {
                executeCoordinator(coordinator2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Iniciar ambos procesos en paralelo
        process1.start();
        process2.start();

        // Esperar a que ambos procesos terminen
        process1.join();
        process2.join();

        // Combinar los resultados finales de ambos coordinadores
        String[] reduceFiles = {
            "/Users/alexperez/Documents/GitHub/DM1/M_5/MapReduce/src/Files/Chunks/MapReduce1/reduce_1.txt",
            "/Users/alexperez/Documents/GitHub/DM1/M_5/MapReduce/src/Files/Chunks/MapReduce1/reduce_2.txt",
            "/Users/alexperez/Documents/GitHub/DM1/M_5/MapReduce/src/Files/Chunks/MapReduce2/reduce_1.txt",
            "/Users/alexperez/Documents/GitHub/DM1/M_5/MapReduce/src/Files/Chunks/MapReduce2/reduce_2.txt"
        };
        
        FinalReduceNode finalReduceNode = new FinalReduceNode(reduceFiles, "/Users/alexperez/Documents/GitHub/DM1/M_5/MapReduce/src/Files/final_result.txt");
        finalReduceNode.combineReduceResults(); // Combinar los resultados en un solo archivo final
    }

    private static void executeCoordinator(Coordinator coordinator) throws Exception {
        coordinator.startProcessing();

        List<String> chunks = coordinator.split();
        coordinator.executeMap(chunks);
        coordinator.executeShuffle(chunks.size());
        coordinator.executeReduce();
    }
}
