package mapreduce;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FaultHandler {

    private ExecutorService executor;
    private int retries;

    public FaultHandler(int maxThreads, int retries) {
        this.executor = Executors.newFixedThreadPool(maxThreads);
        this.retries = retries;
    }

    public void executeWithRetry(Runnable task) {
        for (int i = 0; i < retries; i++) {
            try {
                executor.execute(task);
                return; // Si la tarea se ejecuta sin excepciones, sale del método
            } catch (RuntimeException e) {
                System.err.println("Error en la ejecución del nodo. Intento " + (i + 1) + " de " + retries);
                e.printStackTrace();
            }
        }
        System.err.println("Error: la tarea ha fallado después de " + retries + " intentos.");
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
