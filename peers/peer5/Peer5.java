import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

public class Peer5 {
    private static int totalChunks;
    private static int selfPort, downPort;
    private static Set<String> sChunkID = Collections.synchronizedSet(new HashSet<>());
    private static String filePath;

    public static void main(String[] args) throws Exception {
        int foPort = Integer.parseInt(args[0]);
        selfPort = Integer.parseInt(args[1]);
        downPort = Integer.parseInt(args[2]);

        // 1. Initial Download from File Owner
        try (Socket foSocket = new Socket("localhost", foPort);
             ObjectOutputStream out_fo = new ObjectOutputStream(foSocket.getOutputStream());
             ObjectInputStream in_fo = new ObjectInputStream(foSocket.getInputStream())) {

            filePath = (String) in_fo.readObject();
            totalChunks = Integer.parseInt((String) in_fo.readObject());
            List<String> chunkPartList = (List<String>) in_fo.readObject();
            String[] fileSizeList = (String[]) in_fo.readObject();

            for (int i = 0; i < chunkPartList.size(); i++) {
                downloadChunk(chunkPartList.get(i), Integer.parseInt(fileSizeList[i]), in_fo);
            }
        }

        saveChunkList();

        // 2. P2P Swarming Phase
        try (ServerSocket upListener = new ServerSocket(selfPort)) {
            new Thread(() -> startDownloading()).start(); // Background downloader

            while (sChunkID.size() < totalChunks) {
                Socket client = upListener.accept();
                new Upload(client).start();
            }
        }
    }

    private static void downloadChunk(String name, int size, ObjectInputStream in) throws IOException {
        byte[] buffer = new byte[8192];
        try (FileOutputStream fos = new FileOutputStream(name)) {
            int totalRead = 0;
            while (totalRead < size) {
                // Read chunks of bytes via ObjectStream to avoid corruption
                byte[] chunk = (byte[]) in.readObject(); 
                fos.write(chunk);
                totalRead += chunk.length;
            }
        } catch (ClassNotFoundException e) { e.printStackTrace(); }
        sChunkID.add(name);
    }

    // Simplified File Operations
    private static synchronized void saveChunkList() throws IOException {
        Files.write(Paths.get("ChunkIDList.txt"), sChunkID);
    }

    private static synchronized void updateFileChunkIDList(String chunk) {
        try {
            Files.write(Paths.get("ChunkIDList.txt"), (chunk + "\n").getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) { e.printStackTrace(); }
    }

    // --- Upload Logic ---
    private static class Upload extends Thread {
        private Socket socket;
        public Upload(Socket s) { this.socket = s; }

        public void run() {
            try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
                
                while (true) {
                    out.writeObject(new HashSet<>(sChunkID)); // Send current inventory
                    out.flush();

                    String request = (String) in.readObject();
                    if (request.equals("equal sets")) continue;

                    File file = new File(request);
                    out.writeObject(Long.toString(file.length()));
                    out.writeObject(Files.readAllBytes(file.toPath())); // Simplified for small chunks
                    out.flush();
                }
            } catch (Exception e) { /* Connection closed */ }
        }
    }

    // --- Download Logic ---
    private static void startDownloading() {
        while (sChunkID.size() < totalChunks) {
            try (Socket socket = new Socket("localhost", downPort);
                 ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                while (sChunkID.size() < totalChunks) {
                    HashSet<String> neighborChunks = (HashSet<String>) in.readObject();
                    neighborChunks.removeAll(sChunkID);

                    if (neighborChunks.isEmpty()) {
                        out.writeObject("equal sets");
                        Thread.sleep(1000);
                        continue;
                    }

                    String target = neighborChunks.iterator().next();
                    out.writeObject(target);

                    int size = Integer.parseInt((String) in.readObject());
                    byte[] data = (byte[]) in.readObject();
                    Files.write(Paths.get(target), data);
                    
                    sChunkID.add(target);
                    updateFileChunkIDList(target);
                }
            } catch (Exception e) {
                try { Thread.sleep(2000); } catch (InterruptedException ex) {}
            }
        }
        mergeFiles();
    }

    private static void mergeFiles() {
        try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(filePath))) {
            for (int i = 1; i <= totalChunks; i++) {
                Files.copy(Paths.get(String.valueOf(i)), out);
            }
            System.out.println("File fully merged!");
        } catch (IOException e) { e.printStackTrace(); }
    }
}