import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

public class FileOwner {
    private static int sPort;
    private static final List<String> chunkIDs = new ArrayList<>();
    private static String filePath;
    private static final int CHUNK_SIZE = 1024 * 100; // 100 KB

    public static void main(String[] args) throws Exception {
        sPort = 5000;
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter file path: ");
        filePath = scanner.nextLine();
        File fileToSplit = new File(filePath);

        // 1. Split File into Chunks
        splitFile(fileToSplit);

        // 2. Distribute Chunks into 5 parts
        List<String>[] partitions = partitionChunks(5);

        // 3. Start Server
        try (ServerSocket listener = new ServerSocket(sPort)) {
            System.out.println("File Owner running on port " + sPort);
            int peerCount = 0;
            while (true) {
                Socket connection = listener.accept();
                peerCount++;
                // Use modulo to wrap around if more than 5 peers connect
                List<String> peerPart = partitions[(peerCount - 1) % 5];
                new Handler(connection, peerCount, peerPart).start();
            }
        }
    }

    private static void splitFile(File f) throws IOException {
        byte[] buffer = new byte[CHUNK_SIZE];
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
            int bytesRead, partNum = 1;
            while ((bytesRead = bis.read(buffer)) > 0) {
                String name = String.valueOf(partNum++);
                chunkIDs.add(name);
                // Save chunk to disk
                Files.write(Paths.get(name), Arrays.copyOfRange(buffer, 0, bytesRead));
            }
        }
    }

    private static List<String>[] partitionChunks(int n) {
        List<String>[] parts = new ArrayList[n];
        for (int i = 0; i < n; i++) parts[i] = new ArrayList<>();
        for (int i = 0; i < chunkIDs.size(); i++) {
            parts[i % n].add(chunkIDs.get(i));
        }
        return parts;
    }

    private static class Handler extends Thread {
        private final Socket socket;
        private final int id;
        private final List<String> myChunks;

        public Handler(Socket socket, int id, List<String> chunks) {
            this.socket = socket;
            this.id = id;
            this.myChunks = chunks;
        }

        public void run() {
            // Use ONLY Object streams to avoid header/buffer corruption
            try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                
                System.out.println("Peer " + id + " connected. Sending " + myChunks.size() + " chunks.");

                out.writeObject(filePath);
                out.writeObject(String.valueOf(chunkIDs.size()));
                out.writeObject(new ArrayList<>(myChunks));

                // Send sizes
                String[] sizes = new String[myChunks.size()];
                for (int i = 0; i < myChunks.size(); i++) {
                    sizes[i] = String.valueOf(new File(myChunks.get(i)).length());
                }
                out.writeObject(sizes);

                // Send Chunks as Byte Arrays
                for (String chunkName : myChunks) {
                    byte[] fileData = Files.readAllBytes(Paths.get(chunkName));
                    out.writeObject(fileData); // This matches the refined Peer code
                }
                out.flush();
                
            } catch (IOException e) {
                System.err.println("Peer " + id + " disconnected: " + e.getMessage());
            }
        }
    }
}