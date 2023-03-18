package com.trix.man.batch.logic;

import com.trix.man.batch.model.User;
import com.trix.man.batch.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class UserDBWriter implements ItemWriter<User>{
    private static final Logger LOGGER = LoggerFactory.getLogger(UserDBWriter.class);
    private final UserRepository userRepository;
    public UserDBWriter(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Override
    public void write(final List<? extends User> items) throws Exception {
        for (User item : items) {
            LOGGER.info(" --> Writing: {}", item.getId());
        }
        userRepository.saveAll(items);
        userRepository.flush();
    }


}

