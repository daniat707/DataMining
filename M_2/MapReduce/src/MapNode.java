import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class MapNode implements Callable<Map<String, List<Integer>>> {

    private String line;

    public MapNode(String line) {
        this.line = line;
    }

    @Override
    public Map<String, List<Integer>> call() {
        Map<String, List<Integer>> wordCount = new TreeMap<>();
        String[] words = line.split("\\s+");

        for (String word : words) {
            wordCount.computeIfAbsent(word, k -> new ArrayList<>()).add(1);
        }

        return wordCount;
    }
}
