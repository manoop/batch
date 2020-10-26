package com.trix.man.batch.logic;

import com.trix.man.batch.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import javax.annotation.PreDestroy;
import java.io.*;
import java.util.List;

public class UserDBWriter implements ItemWriter<User>, Closeable{
    private final PrintWriter writer;
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDBWriter.class);

    public UserDBWriter() {
        OutputStream out;
        try {
            out = new FileOutputStream("output.txt");
        } catch (FileNotFoundException e) {
            out = System.out;
        }
        this.writer = new PrintWriter(out);
    }

    @Override
    public void write(final List<? extends User> items) throws Exception {
        for (User item : items) {
            LOGGER.info(" --> Writing: {}", item.getId());
            writer.println(item.toString());
        }
    }

    @PreDestroy
    @Override
    public void close() throws IOException {
        writer.close();
    }
}

