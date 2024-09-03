import java.util.Map;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class ReduceNode implements Callable<Map<String, Integer>> {

    private Map<String, List<Integer>> shuffledData;

    public ReduceNode(Map<String, List<Integer>> shuffledData) {
        this.shuffledData = shuffledData;
    }

    @Override
    public Map<String, Integer> call() {
        Map<String, Integer> finalCounts = new TreeMap<>();
        for (Map.Entry<String, List<Integer>> entry : shuffledData.entrySet()) {
            int sum = entry.getValue().stream().mapToInt(Integer::intValue).sum();
            finalCounts.put(entry.getKey(), sum);
        }

        return finalCounts;
    }
}
