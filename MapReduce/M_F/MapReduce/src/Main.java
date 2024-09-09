import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws Exception {
        String ruta = "/Users/alexperez/Documents/GitHub/DM1/M_F/Input.txt";

        // Tamaño de cada chunk
        int chunkSize = 32 * 1000 * 1000;

        // Solicitar errores al usuario
        Scanner scanner = new Scanner(System.in);
        System.out.print("Inducir un error en el coordinador? (S/N): ");
        boolean induceCoordinatorError = scanner.nextLine().trim().equalsIgnoreCase("S");

        // Si el coordinador falla, el programa se termina
        if (induceCoordinatorError) {
            System.out.println("\u001B[31mEl coordinador ha fallado. Terminando programa...\u001B[0m");
            System.exit(1);
        }

        // Inducir fallos en los nodos
        System.out.print("Inducir un error en el nodo Map? (S/N): ");
        boolean induceMapError = scanner.nextLine().trim().equalsIgnoreCase("S");

        System.out.print("Inducir un error en el nodo Shuffle? (S/N): ");
        boolean induceShuffleError = scanner.nextLine().trim().equalsIgnoreCase("S");

        System.out.print("Inducir un error en el nodo Reduce? (S/N): ");
        boolean induceReduceError = scanner.nextLine().trim().equalsIgnoreCase("S");

        System.out.print("Inducir un error en el Nodo Final Reduce? (S/N): ");
        boolean induceFinalReduceError = scanner.nextLine().trim().equalsIgnoreCase("S");

        System.out.print("¿En qué coordinador quieres inducir el error (1 o 2)? ");
        int errorCoordinator = scanner.nextInt();

        // Coordinador 1 procesa los primeros 20 chunks, comenzando en chunk 0
        Coordinator coordinator1 = new Coordinator(chunkSize, ruta, "MapReduce1/", 20, 0, 0, false, induceMapError && errorCoordinator == 1, induceShuffleError && errorCoordinator == 1, induceReduceError && errorCoordinator == 1);

        // Coordinador 2 procesa los siguientes 21 chunks, comenzando en chunk 20
        Coordinator coordinator2 = new Coordinator(chunkSize, ruta, "MapReduce2/", 21, chunkSize * 20, 20, false, induceMapError && errorCoordinator == 2, induceShuffleError && errorCoordinator == 2, induceReduceError && errorCoordinator == 2);

        // Procesar en paralelo
        Thread process1 = new Thread(() -> {
            try {
                executeCoordinator(coordinator1, induceMapError && errorCoordinator == 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread process2 = new Thread(() -> {
            try {
                executeCoordinator(coordinator2, induceMapError && errorCoordinator == 2);
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
            "/Users/alexperez/Documents/GitHub/DM1/M_F/MapReduce/src/Files/Chunks/MapReduce1/reduce_1.txt",
            "/Users/alexperez/Documents/GitHub/DM1/M_F/MapReduce/src/Files/Chunks/MapReduce1/reduce_2.txt",
            "/Users/alexperez/Documents/GitHub/DM1/M_F/MapReduce/src/Files/Chunks/MapReduce2/reduce_1.txt",
            "/Users/alexperez/Documents/GitHub/DM1/M_F/MapReduce/src/Files/Chunks/MapReduce2/reduce_2.txt"
        };
        
        FinalReduceNode finalReduceNode = new FinalReduceNode(reduceFiles, "/Users/alexperez/Documents/GitHub/DM1/M_F/MapReduce/src/Files/final_result.txt", induceFinalReduceError);
        finalReduceNode.combineReduceResults(); // Combinar los resultados en un solo archivo final
    }

    private static void executeCoordinator(Coordinator coordinator, boolean induceMapError) throws Exception {
        coordinator.startProcessing();

        List<String> chunks = coordinator.split();
        coordinator.executeMap(chunks);

        if (induceMapError) {
            System.out.println("\u001B[31mEl nodo Map ha fallado. Reiniciando nodo Map...\u001B[0m");
            Thread.sleep(5000); // Simula el tiempo de reinicio del nodo Map
            System.out.println("\u001B[32mNodo Map reiniciado. Continuando procesamiento...\u001B[0m");
        }

        coordinator.executeShuffle(chunks.size());
        coordinator.executeReduce();
    }
}
