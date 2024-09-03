import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        // Simulación de datos de entrada
        List<String> input = Arrays.asList("hola mundo", "hola de nuevo mundo");

        // Crear y ejecutar el coordinador de MapReduce
        Coordinator coordinator = new Coordinator(input);
        coordinator.execute();
    }
}