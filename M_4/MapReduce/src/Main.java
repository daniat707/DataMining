import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Main {
    public static void main(String[] args) throws Exception {
        String ruta = "/Users/alexperez/Documents/GitHub/DM1/input.txt";

        Coordinator coordinator = new Coordinator(32 * 1000 * 1000, ruta);
        coordinator.startProcessing();

        List<String> chunks = coordinator.split();

        coordinator.executeMap(chunks);
        coordinator.executeShuffle();
        coordinator.executeReduce();
    }
}

