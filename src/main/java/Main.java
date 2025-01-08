import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.nio.charset.StandardCharsets;

public class Main {

    private static final Map<String, ExpiryAndValue> cache = new HashMap<>();

    public static class ExpiryAndValue {
        final long expiryTimestamp;
        final String value;

        public ExpiryAndValue(String value) {
            this.expiryTimestamp = Long.MAX_VALUE;
            this.value = value;
        }

        public ExpiryAndValue(long expiryTimestamp, String value) {
            this.expiryTimestamp = expiryTimestamp;
            this.value = value;
        }
    }

//  private static final ConcurrentHashMap<String, String> setDict = new ConcurrentHashMap<>();

/*
    public static List<Object> parserCommand(BufferedReader in) {
        List<Object> ret = new ArrayList<>();
        try {
            String line1 = in.readLine();
            if (line1 == null) {
                ret.add("exit");
                return ret;
            }
            if (line1.charAt(0) != '*') {
                throw new RuntimeException("ERR, command must be an array! ");
            }
            int nEle = Integer.parseInt(line1.substring(1));
            in.readLine();
            ret.add(in.readLine());
            System.out.println("Received command: " + ret.getFirst() + ", number of elements: " + nEle);
            String line;
            for (int i = 1; i < nEle && (line = in.readLine()) != null; i++) {
                if (line.isEmpty()) {
                    continue;
                }
                char type = line.charAt(0);
                switch (type) {
                    case '$':
                        System.out.println("parse bulk string: " + line);
                        ret.add(line);
                        ret.add(in.readLine());
                        break;
                    case ':':
                        System.out.println("parse int: " + line);
                        ret.add(String.valueOf(type));
                        ret.add(Integer.parseInt(line.substring(1)));
                        break;
                    default:
                        System.out.println("default: " + line);
                        break;
                }
            }
        } catch (IOException e) {
            System.out.println("Parse failed, " + e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            System.out.println("Parse failed, " + e.getMessage());
        }
        System.out.println("Command: " + String.join(" ", ret.stream().toArray(String[]::new)));
        return ret;
    }
*/

    public static void main(String[] args) throws IOException {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");

    /*
        Uncomment this block to pass the first stage
        ServerSocket;
        Socket clientSocket = null;
    */

        int port = 6379;

        Selector selector = Selector.open();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        //  ExecutorService es = null;

        try (ServerSocketChannel serverSocket = ServerSocketChannel.open()) {
        /*
            serverSocket = new ServerSocket(port);
            Since the tester restarts your program quite often, setting SO_REUSEADDR
            ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);

            Wait for connection from client.
            clientSocket = serverSocket.accept();

            Task #2
            clientSocket.getOutputStream().write("+PONG\r\n".getBytes());

            Task #3
            processRequest(clientSocket);

            Task #4
            while (true) {
                clientSocket = serverSocket.accept();
                Socket finalClientSocket = clientSocket;
                new Thread(() -> {
                    try {
                        handlePingCommand(finalClientSocket);
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }).start();
            }

            Task #5
            while (true) {
                clientSocket = serverSocket.accept();
                Socket finalClientSocket = clientSocket;
                new Thread(() -> {
                    InputStream input;
                    try {
                        input = finalClientSocket.getInputStream();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    String line = "";
                    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                    while (!line.equals("QUIT")) {
                        try {
                            line = reader.readLine();
                            if (line != null && line.equalsIgnoreCase("ping")) {
                                finalClientSocket.getOutputStream().write("+PONG\r\n".getBytes());
                            } else if (line != null && line.equalsIgnoreCase("echo")) {
                                reader.readLine();
                                String message = reader.readLine();
                                finalClientSocket.getOutputStream().write(String.format("$%d\r\n%s\r\n", message.length(), message).getBytes());
                            }
                        } catch (SocketException e) {
                            System.out.println("SocketException: Connection reset");
                            break;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            }

            Task #6
            es = Executors.newFixedThreadPool(4);
            class Handler implements Runnable {
                final Socket clientSocket;
                public Handler(Socket sock) {
                    clientSocket = sock;
                }
                @Override
                public void run() {
                    try (OutputStreamWriter out = new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8);
                         BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                        List<Object> command;
                        while ((command = parserCommand(in)) != null) {
                            String cmd = (String)command.getFirst();
                            System.out.println("Responding: " + cmd);
                            String key;
                            switch (cmd.toLowerCase()) {
                                case "ping":
                                    out.write("+PONG\r\n");
                                    break;
                                case "echo":
                                    out.write(String.join("\r\n", command.stream().skip(1).toArray(String[] ::new)) + "\r\n");
                                    break;
                                case "set":
                                    out.write("+OK\r\n");
                                    key = (String)command.get(2);
                                    setDict.put(key, String.join("\r\n", command.stream().skip(3).toArray(String[] ::new)) + "\r\n");
                                    break;
                                case "get":
                                    key = (String)command.get(2);
                                    out.write(setDict.getOrDefault(key, "$-1\r\n"));
                                    break;
                                default:
                                    return;
                            }
                            out.flush();
                        }
                    } catch (IOException e) {
                        System.out.println("IOException: " + e.getMessage());
                    } catch (RuntimeException e) {
                        System.out.println("RuntimeException: " + e.getMessage());
                    }
                }
            }
            while ((clientSocket = serverSocket.accept()) != null) {
                es.submit(new Handler(clientSocket));
            }
        */

        //  Task #7
            serverSocket.bind(new InetSocketAddress("localhost", port));
            serverSocket.configureBlocking(false);
            serverSocket.register(selector, SelectionKey.OP_ACCEPT);

            while (true) {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                for (SelectionKey key : selectedKeys) {
                    if (key.isAcceptable()) {
                        SocketChannel client = serverSocket.accept();
                        client.configureBlocking(false);
                        client.register(selector, SelectionKey.OP_READ);
                    }
                    if (key.isReadable()) {
                        buffer.clear();
                        SocketChannel client = (SocketChannel) key.channel();
                        int bytesRead = client.read(buffer);
                        if (bytesRead == -1) {
                            continue;
                        }
                        buffer.flip();
                        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
                        System.out.println("command: " + charBuffer);
                        List<String> parsedCommand = parseCommand(charBuffer);
                        buffer.clear();
                        processCommand(parsedCommand, buffer);
                        buffer.flip();
                        client.write(buffer);
                    }
                }
                selectedKeys.clear();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    static void processCommand(List<String> parsedCommand, ByteBuffer buffer) {
        String cmd = parsedCommand.get(0);
        String response = "+ERROR\n";
        if (cmd.equalsIgnoreCase("PING")) {
            response = "+PONG\r\n";
        } else if (cmd.equalsIgnoreCase("ECHO")) {
            response = "+" + parsedCommand.get(1) + "\r\n";
        } else if (cmd.equalsIgnoreCase("SET")) {
            ExpiryAndValue toStore;
            if (parsedCommand.size() > 3 && parsedCommand.get(3).equalsIgnoreCase("PX")) {
                int milliS = Integer.parseInt(parsedCommand.get(4));
                long expiryTimestamp = System.currentTimeMillis() + milliS;
                toStore = new ExpiryAndValue(expiryTimestamp, parsedCommand.get(2));
            } else {
                toStore = new ExpiryAndValue(parsedCommand.get(2));
            }
            cache.put(parsedCommand.get(1), toStore);
            response = "+OK\r\n";
        } else if (cmd.equalsIgnoreCase("GET")) {
            ExpiryAndValue cached = cache.get(parsedCommand.get(1));
            if (cached != null && cached.expiryTimestamp >= System.currentTimeMillis()) {
                String content = cached.value;
                response = "$" + content.length() + "\r\n" + content + "\r\n";
            } else {
                response = "$-1\r\n";
            }
        }
        buffer.put(response.getBytes(StandardCharsets.UTF_8));
    }

    static List<String> parseCommand(CharBuffer data) {
        Tokenizer tokenizer = new Tokenizer(data);
        tokenizer.chomp('*');
        int arraySize = tokenizer.nextInt();
        tokenizer.chomp('\r');
        tokenizer.chomp('\n');
        if (arraySize == 0) {
            System.out.println("Empty array, was that intended?");
            return Collections.emptyList();
        }
        List<String> args = new ArrayList<>();
        for (int i = 0; i < arraySize; i++) {
            tokenizer.chomp('$');
            int argLength = tokenizer.nextInt();
            tokenizer.chomp('\r');
            tokenizer.chomp('\n');
            String arg = tokenizer.nextString(argLength);
            tokenizer.chomp('\r');
            tokenizer.chomp('\n');
            args.add(arg);
        }
        System.out.println("args: " + args);
        return args;
    }

    private static class Tokenizer {
        final CharBuffer buf;

        Tokenizer(CharBuffer buf) {
            this.buf = buf.asReadOnlyBuffer();
        }

        void chomp(char expected) {
            if (buf.get() != expected) {
                throw new IllegalStateException(buf + "[" + (buf.position() - 1) + "] != " + expected);
            }
        }

        int nextInt() {
            int start = buf.position();
            char curr = buf.get();
            StringBuilder sb = new StringBuilder();
            while (curr >= '0' && curr <= '9') {
                sb.append(curr);
                curr = buf.get();
            }
            buf.position(buf.position() - 1);
            if (buf.position() == start) {
                throw new IllegalStateException(buf + "[" + start + "] is not an int!");
            }
            return Integer.parseInt(sb.toString());
        }

        String nextString(int len) {
            if (buf.remaining() < len) {
                throw new IllegalStateException("Out of bounds read!");
            }
            char[] chars = new char[len];
            buf.get(chars, 0, len);
            return new String(chars);
        }
    }

/*
    private static void handlePingCommand(Socket finalClientSocket) {
        OutputStream outputStream = null;
        BufferedReader inputStream = null;

        try {
            outputStream = finalClientSocket.getOutputStream();
            inputStream = new BufferedReader(new InputStreamReader(finalClientSocket.getInputStream()));
            String content;
            while ((content = inputStream.readLine()) != null) {
                if ("ping".equalsIgnoreCase(content)) {
                    outputStream.write(("+PONG\r\n").getBytes());
                    outputStream.flush();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (inputStream != null) {
                    outputStream.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                    finalClientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
    private static void processRequest(Socket clientSocket) {
        try (BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()));
             BufferedWriter bufferedWriter = new BufferedWriter(
                     new OutputStreamWriter(clientSocket.getOutputStream()));) {
            String content;
            while ((content = bufferedReader.readLine()) != null) {
                System.out.println("::" + content);
                if ("ping".equalsIgnoreCase(content)) {
                    bufferedWriter.write("+PONG\r\n");
                    bufferedWriter.flush();
                } else if ("eof".equalsIgnoreCase(content)) {
                    System.out.println("eof");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
*/

}