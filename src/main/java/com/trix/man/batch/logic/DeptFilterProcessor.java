package com.trix.man.batch.logic;

import com.trix.man.batch.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;


public class DeptFilterProcessor implements ItemProcessor<User, User> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeptFilterProcessor.class);

    public DeptFilterProcessor() {
    }

    @Override
    public User process(User item) throws Exception {
        item.setStatus("PROCESSED");
        LOGGER.info("Proccessed item {}", item.getId());
        return item;
    }
}
