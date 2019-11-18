
import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class WordCountWorker {
    public static void main(String[] args) throws IOException {
        int heartBeatPort = Integer.parseInt(args[0]);
        int wordCountPort = Integer.parseInt(args[1]);
        // Threaded
        CyclicBarrier barrier = new CyclicBarrier(3);
        Thread heartBeat = new Thread(new Runnable() {
            @Override
            public void run() {
                Socket wClientSocket = null;
                try {
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                    wClientSocket = new Socket("127.0.0.1", heartBeatPort);
                    BufferedReader wIn = new BufferedReader(new InputStreamReader(wClientSocket.getInputStream()));
                    PrintWriter wOut = new PrintWriter(wClientSocket.getOutputStream(), true);
                    String message = wIn.readLine();
                    while (true) {
                        System.out.println("W-> "+ message);
                        if (message.contains(heartBeatPort + " Alive?")) {
                            wOut.println(heartBeatPort + " Yes");
                        }
                        else if (message.contains(" Die")) {
                            break;
                        }
                        else {
                            wOut.println(heartBeatPort + " Waiting");
                        }
                        message = wIn.readLine();
                    }
                    wIn.close();
                    wOut.close();
                    wClientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread wordCount = new Thread(){
            @Override
            public void run() {
                try {
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                    Socket wClientSocket = new Socket("127.0.0.1", wordCountPort);
                    DataInputStream wIn = new DataInputStream(wClientSocket.getInputStream());
                    DataOutputStream wOut = new DataOutputStream(wClientSocket.getOutputStream());
                    wOut.writeUTF(wordCountPort + ": Started");
                    wOut.flush();

                    String filename = null;
                    String message = wIn.readUTF();

                    while (true) {
                        System.out.println(message);
                        if (message.contains(wordCountPort + " File")) {
                            filename = message.split(": ")[1];

                            File inputFile = new File(filename);
                            String filePath = "wordCount-output" + filename.substring(filename.lastIndexOf('/'), filename.lastIndexOf('.'));
                            File outputFile = new File(filePath.concat("-output-" + wordCountPort + ".txt"));
                            BufferedReader br = new BufferedReader(new FileReader(inputFile));
                            BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
                            String line;
                            Map<String, Integer> wordMap = new HashMap<>();
                            while ((line = br.readLine()) != null) {
                                String[] words = line.split(" ");
                                for (String w : words) {
                                    if (!wordMap.containsKey(w))
                                        wordMap.put(w, 1);
                                    else {
                                        int count = wordMap.get(w);
                                        wordMap.put(w, count + 1);
                                    }
                                }
                            }
                            wordMap.remove("");
                            System.out.println("W-> "+ wordMap.toString());
                            bw.write(wordMap.toString());
                            wOut.writeUTF(wordCountPort + ": Finished " + filename.substring(filename.lastIndexOf('/')));
                            wOut.flush();
                            bw.close();
                            br.close();
                        }
                        else if (message.contains("Processed")) {
                            System.out.println("Master done");
                            wClientSocket.close();
                            break;
                        }
                        message = wIn.readUTF();
                    }
                    Thread.sleep(2000);
                    wIn.close();
                    wOut.close();
                    wClientSocket.close();

                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }};
        heartBeat.start();
        wordCount.start();
        try {
            barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }
}
