import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

public class MapNode implements Callable<Map<String, List<Integer>>> {

    private List<String> chunkFiles;
    private String outputFilePath;
    private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
        "the", "ab", "and", "of", "to", "in", "a", "was", "he", "it", "his", "that", "with", "for", "had", "as", "at", "by", 
        "on", "not", "b", "be", "is", "were", "but", "from", "which", "or", "this", "have", "him", "all", "her", "so", 
        "when", "no", "if", "would", "out", "about", "there", "been", "more", "one", "who", "up", "their", "could", 
        "what", "some", "into", "said", "than", "any", "only", "where", "every", "other", "through", "before", "these", 
        "after", "should", "again", "over", "down", "those", "because", "such", "while", "then", "they", "them", 
        "were", "until", "very", "having", "upon", "against", "each", "during", "whether", "however", "even", "nor", 
        "though", "thus", "once", "never", "ever", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
        "ab", "ac", "ad", "ae", "af", "ag", "ah", "ai", "aj", "ak", "al", "am", "an", "ao", "ap", "aq", "ar", "as", "at", "au", "av", "aw", "ax", "ay", "az",
        "bc", "bd", "be", "bf", "bg", "bh", "bi", "bj", "bk", "bl", "bm", "bn", "bo", "bp", "bq", "br", "bs", "bt", "bu", "bv", "bw", "bx", "by", "bz",
        "cd", "ce", "cf", "cg", "ch", "ci", "cj", "ck", "cl", "cm", "cn", "co", "cp", "cq", "cr", "cs", "ct", "cu", "cv", "cw", "cx", "cy", "cz",
        "de", "df", "dg", "dh", "di", "dj", "dk", "dl", "dm", "dn", "do", "dp", "dq", "dr", "ds", "dt", "du", "dv", "dw", "dx", "dy", "dz",
        "ef", "eg", "eh", "ei", "ej", "ek", "el", "em", "en", "eo", "ep", "eq", "er", "es", "et", "eu", "ev", "ew", "ex", "ey", "ez",
        "fg", "fh", "fi", "fj", "fk", "fl", "fm", "fn", "fo", "fp", "fq", "fr", "fs", "ft", "fu", "fv", "fw", "fx", "fy", "fz",
        "gh", "gi", "gj", "gk", "gl", "gm", "gn", "go", "gp", "gq", "gr", "gs", "gt", "gu", "gv", "gw", "gx", "gy", "gz",
        "hi", "hj", "hk", "hl", "hm", "hn", "ho", "hp", "hq", "hr", "hs", "ht", "hu", "hv", "hw", "hx", "hy", "hz",
        "ij", "ik", "il", "im", "in", "io", "ip", "iq", "ir", "is", "it", "iu", "iv", "iw", "ix", "iy", "iz",
        "jk", "jl", "jm", "jn", "jo", "jp", "jq", "jr", "js", "jt", "ju", "jv", "jw", "jx", "jy", "jz",
        "kl", "km", "kn", "ko", "kp", "kq", "kr", "ks", "kt", "ku", "kv", "kw", "kx", "ky", "kz",
        "lm", "ln", "lo", "lp", "lq", "lr", "ls", "lt", "lu", "lv", "lw", "lx", "ly", "lz",
        "mn", "mo", "mp", "mq", "mr", "ms", "mt", "mu", "mv", "mw", "mx", "my", "mz",
        "no", "np", "nq", "nr", "ns", "nt", "nu", "nv", "nw", "nx", "ny", "nz",
        "op", "oq", "or", "os", "ot", "ou", "ov", "ow", "ox", "oy", "oz",
        "pq", "pr", "ps", "pt", "pu", "pv", "pw", "px", "py", "pz",
        "qr", "qs", "qt", "qu", "qv", "qw", "qx", "qy", "qz",
        "rs", "rt", "ru", "rv", "rw", "rx", "ry", "rz",
        "st", "su", "sv", "sw", "sx", "sy", "sz",
        "tu", "tv", "tw", "tx", "ty", "tz",
        "uv", "uw", "ux", "uy", "uz",
        "vw", "vx", "vy", "vz",
        "wx", "wy", "wz",
        "xy", "xz",
        "yz"
    ));

    public MapNode(List<String> chunkFiles, String outputFilePath) {
        this.chunkFiles = chunkFiles;
        this.outputFilePath = outputFilePath;
    }

    @Override
    public Map<String, List<Integer>> call() {
        Map<String, List<Integer>> wordCount = new TreeMap<>();
        for (String chunkFile : chunkFiles) {
            try (BufferedReader br = new BufferedReader(new FileReader(chunkFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // Limpiar la línea eliminando todo lo que no sea letras y pasando a minúsculas
                    line = line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase();
                    String[] words = line.split("\\s+");
                    for (String word : words) {
                        if (!word.isEmpty() && !STOPWORDS.contains(word)) {
                            // Cada palabra es independiente, no acumulamos aquí
                            wordCount.computeIfAbsent(word, k -> new ArrayList<>()).add(1);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        saveMapOutput(wordCount, outputFilePath);
        return wordCount;
    }

    private void saveMapOutput(Map<String, List<Integer>> wordCount, String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            for (Map.Entry<String, List<Integer>> entry : wordCount.entrySet()) {
                for (Integer count : entry.getValue()) {
                    writer.write("(" + entry.getKey() + ", " + count + ")\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}