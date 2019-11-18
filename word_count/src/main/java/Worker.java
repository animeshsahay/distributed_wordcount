import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Worker implements Runnable {

    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    private String id;

    public Worker(String id, Socket clientSocket) throws IOException {
        this.id = id;
        this.clientSocket = clientSocket;
        out = new PrintWriter(clientSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
    }

    @Override
    public void run() {
//        System.out.println("In run");
//        out.println(id + " Hello");
        try {
            String message = in.readLine();
            if (message.contains(id)) {
                out.println(id + "HeartbeatClient");
            }
            //clientSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
