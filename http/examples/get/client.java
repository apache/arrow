import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;

public class ArrowHttpClient {

    public static void main(String[] args) {
        String serverUrl = "http://localhost:8000";

        try {
            URL url = new URL(serverUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                InputStream inputStream = connection.getInputStream();

                BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator);
                List<ArrowRecordBatch> batches = new ArrayList<>();

                int num_rows = 0;
                while (reader.loadNextBatch()) { 
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    num_rows += root.getRowCount();
                    VectorUnloader unloader = new VectorUnloader(root);
                    ArrowRecordBatch arb = unloader.getRecordBatch();
                    batches.add(arb);
                }
                
                System.out.println(reader.bytesRead() + " bytes received");
                System.out.println(num_rows + " records received");
                System.out.println(batches.size() + " record batches received");

                reader.close();
            } else {
                System.err.println("Failed with response code: " + connection.getResponseCode());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
