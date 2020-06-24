package com.pentasecurity.callback;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ReverseStringCallback implements StreamCallback {
    private boolean isSuccess;
    private ComponentLog logger;

    @Override
    public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
        String originalContent = new BufferedReader(new InputStreamReader(inputStream)).readLine();

        try {
            outputStream.write(new StringBuilder(originalContent).reverse().toString().getBytes(StandardCharsets.UTF_8));
            isSuccess = true;
        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }

    }

    public boolean isSuccess() {
        return isSuccess;
    }
}
