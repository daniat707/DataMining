import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        String ruta = "/Users/alexperez/Documents/GitHub/DM1/M_4/Input.txt";

        // Tamaño de cada chunk (en bytes)
        int chunkSize = 32 * 1000 * 1000;

        // Coordinadores para los dos grupos de chunks
        Coordinator coordinator1 = new Coordinator(chunkSize, ruta, "MapReduce1/", 20, 0); // 20 chunks, comenzando desde 0
        Coordinator coordinator2 = new Coordinator(chunkSize, ruta, "MapReduce2/", 21, chunkSize * 20); // 21 chunks, comenzando donde terminó el primer coordinador

        // Procesar en paralelo
        Thread process1 = new Thread(() -> {
            try {
                executeCoordinator(coordinator1); // Procesa los primeros 20 chunks
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread process2 = new Thread(() -> {
            try {
                executeCoordinator(coordinator2); // Procesa los 21 chunks restantes
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
    }

    // Método auxiliar para ejecutar un coordinador
    private static void executeCoordinator(Coordinator coordinator) throws Exception {
        coordinator.startProcessing();

        List<String> chunks = coordinator.split();
        coordinator.executeMap(chunks); // Procesar los chunks del coordinador correspondiente
        coordinator.executeShuffle(chunks.size()); // Pasar la cantidad de archivos que generó
        coordinator.executeReduce();
    }
}
