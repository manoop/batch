package com.trix.man.batch.logic;

import com.trix.man.batch.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import java.util.HashMap;
import java.util.Map;

public class DeptFilterProcessor implements ItemProcessor<User, User> {

    private static final Map<String, String> DEPT = new HashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(DeptFilterProcessor.class);

    public DeptFilterProcessor() {
        DEPT.put("001", "PB");
        DEPT.put("002", "WM");
        DEPT.put("003", "PCC");
    }

    @Override
    public User process(User item) throws Exception {
        LOGGER.info("*** Processing: {} with thread_id {}", item.getId(), Thread.currentThread().getId());
        Thread.sleep(5000);
        return item;
    }
}
