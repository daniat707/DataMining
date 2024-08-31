package mapreduce;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileSplitter {

    private String filePath;
    private int chunkSize;

    public FileSplitter(String filePath, int chunkSize) {
        this.filePath = filePath;
        this.chunkSize = chunkSize;
    }

    public List<String> split() {
        List<String> chunks = new ArrayList<>();
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(filePath))) {
            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            int chunkCount = 0;

            while ((bytesRead = bis.read(buffer)) != -1) {
                String chunkFileName = "chunk_" + chunkCount + ".txt";
                try (FileOutputStream fos = new FileOutputStream(chunkFileName)) {
                    fos.write(buffer, 0, bytesRead);
                    chunks.add(chunkFileName);
                    chunkCount++;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return chunks;
    }
}
