import java.io.IOException;
import java.net.Socket;

//Thinking this is a master
public class InterProcessCommunication {
    public static void main(String args[]){
        //Creating a worker thread
        ProcessBuilder processBuilder = new ProcessBuilder();
        try {
            Process processWorker1 = processBuilder.start();
            Process processWorker2 = processBuilder.start();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
