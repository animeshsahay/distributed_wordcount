import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;


public class WordCount implements Master {
    private int workerNum;
    private int currSocket = 7777;
    private int[] workerSockets;
    private static Stack<String> stackOfFiles = new Stack<>();
    private Set<Integer> started = new HashSet<>();
    private Set<Integer> aliveWorkers = new HashSet<>();
    private HashMap<Integer, Process> workersMap = new HashMap<>();
    private HashMap<String, Integer> finalSorted = new HashMap<>();
    private HashMap<String, Integer> sorted = new LinkedHashMap<String, Integer>();
    private PrintStream outputStream = new PrintStream(new File("output-file.txt"));
    private File outputFolder = new File("wordCount-output");

    public WordCount(int workerNum, String[] filenames) throws IOException {
        this.workerNum = workerNum;
        for (String f: filenames) {
            stackOfFiles.push(f);
        }
        this.workerSockets = new int[workerNum];
    }

    public void setOutputStream(PrintStream out) {
        this.outputStream = out;
    }

    public static void main(String[] args) throws Exception {
        String[] arr = {"src/test/resources/simple.txt", "src/test/resources/random.txt"};
        WordCount wordCount = new WordCount(1, arr);
        wordCount.run();
    }

    public void run() {
        CyclicBarrier barrier = new CyclicBarrier(3);
        // Creating a new folder for output
        File file = new File("wordCount-output");
        if(file.exists()) {
            File[] children = file.listFiles();
            for (int i = 0; i < children.length; i++) {
                boolean success = children[i].delete();
            }
        }
        file.mkdir();
        for (int i = 0; i < workerNum; i++) {
            workerSockets[i] = currSocket;
            int finalI = i;
            Thread heartbeat = new Thread(){
                @Override
                public void run() {
                    try {
                        try {
                            barrier.await();
                        } catch (InterruptedException | BrokenBarrierException e) {
                            e.printStackTrace();
                        }
//                        Thread.sleep(500);
                        ServerSocket serverSocket = new ServerSocket(workerSockets[finalI]);
                        Socket server = serverSocket.accept();
                        PrintWriter out = new PrintWriter(server.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(server.getInputStream()));

                        while (true) {
                            out.println(workerSockets[finalI] + " Alive?");
                            String clientMessage = in.readLine();
                            if (clientMessage.contains(workerSockets[finalI] + " Yes")) {
                                System.out.println("M-> "+ clientMessage);
                                aliveWorkers.add(workerSockets[finalI]);
                            } else {
                                aliveWorkers.remove(workerSockets[finalI]);
                                break;
                            }
                            if (stackOfFiles.isEmpty() && started.isEmpty()) {
                                break;
                            }
                            System.out.println("Alive: " + aliveWorkers);
                            Thread.sleep(3000);
                        }
                        in.close();
                        out.close();
                        server.close();
                        serverSocket.close();
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };
            Thread wordCount = new Thread(){
                @Override
                public void run() {
                    try {
                        try {
                            barrier.await();
                        } catch (InterruptedException | BrokenBarrierException e) {
                            e.printStackTrace();
                        }
                        int countSocket = workerSockets[finalI]+1;
                        ServerSocket serverSocket = new ServerSocket(countSocket);
                        Socket server = serverSocket.accept();
                        DataOutputStream out = new DataOutputStream(server.getOutputStream());
                        DataInputStream in = new DataInputStream(server.getInputStream());

                        String clientMessage = in.readUTF();

                        System.out.println("M-> "+ stackOfFiles.size());
                        System.out.println(clientMessage);

                        if (stackOfFiles.size() == 0) {
                            in.close();
                            out.close();
                            server.close();
                            serverSocket.close();
                        }
                        else {
                            String filename;
                            while(true) {
                                if(clientMessage.contains(countSocket + ": Started")) {
                                    started.add(countSocket);
                                    System.out.println("M-> "+ stackOfFiles.size());
                                    synchronized (stackOfFiles) {
                                        if (stackOfFiles.size() > 0) {
                                            filename = stackOfFiles.pop();
                                            out.writeUTF(countSocket + " File: " + filename);
                                            out.flush();
                                            System.out.println("M-> "+ "Sent file " + filename);
                                        }
                                    }
                                }
                                else if(clientMessage.contains(countSocket + ": Finished")) {
                                    Thread.sleep(2000);
                                    System.out.println("M-> FinSize "+ stackOfFiles.size());
                                    synchronized (stackOfFiles) {
                                        if (stackOfFiles.size() > 0) {
                                            filename = stackOfFiles.pop();
                                            out.writeUTF(countSocket + " File: " + filename);
                                            out.flush();
                                        }
                                        else
                                            break;
                                    }
                                }
                                clientMessage = in.readUTF();
                                System.out.println("M-> "+ clientMessage);
                            }
                            System.out.println(countSocket + ": Exited");
                            started.remove(countSocket);
                            System.out.println(started);
                            if (stackOfFiles.size() == 0 && started.isEmpty()) {
                                System.out.println("Done");
                                out.writeUTF("Processed");
                                out.flush();
//                               Thread.sleep(2000);
                                in.close();
                                out.close();
                                serverSocket.close();
                                server.close();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            heartbeat.start();
            wordCount.start();
            try {
                barrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

            try {
                createWorker();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        // Creating a thread for combining outputs from all the workers
        Thread combiner = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (stackOfFiles){
                        if (stackOfFiles.isEmpty())
                            break;
                    }
                }
                for(File file : Objects.requireNonNull(outputFolder.listFiles())) {
                    BufferedReader br = null;
                    try {
                        br = new BufferedReader(new FileReader(file));
                        String content = br.readLine();
                        content = content.replace("{", "");
                        content = content.replace("}", "");
                        for (String pair : content.split(", ")) {
                            String key = pair.split("=")[0];
                            Integer value = Integer.parseInt(pair.split("=")[1]);
                            if (finalSorted.containsKey(key)) {
                                value += finalSorted.get(key);
                                finalSorted.put(key, value);
                            } else {
                                finalSorted.put(key, value);
                            }
                        }
                        List<Map.Entry<String, Integer>> list =
                                new LinkedList<Map.Entry<String, Integer>>(finalSorted.entrySet());

                        Collections.sort(list, (o1, o2) -> {
                            if (o1.getValue() < o2.getValue()) {
                                return 1;
                            }
                            else if (o1.getValue() > o2.getValue()) {
                                return -1;
                            }
                            else {
                                return o1.getKey().compareTo(o2.getKey());
                            }
                        });

                        for (Map.Entry<String, Integer> aa : list) {
                            sorted.put(aa.getKey(), aa.getValue());
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                for (Map.Entry<String, Integer> print : sorted.entrySet()) {
                    outputStream.println(print.getValue() + " : " + print.getKey());
                }

            }
        });
        combiner.start();
        try {
            combiner.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Collection<Process> getActiveProcess() {
        LinkedList<Process> activeProcess = new LinkedList<>();
        for (int id : started) {
            activeProcess.add(workersMap.get(id));
        }
        return activeProcess;
    }

    public void createWorker() throws IOException {
        final int port = currSocket;
        currSocket +=2 ;

        System.out.println("Inside createWorker");
        Class klass = WordCountWorker.class;
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = klass.getCanonicalName();
        System.out.println(currSocket);
        ProcessBuilder processBuilder = new ProcessBuilder(javaBin, "-cp", classpath, className, String.valueOf(port), String.valueOf(port+1));
        Process process = processBuilder.start();
        workersMap.put(port, process);
    }
}