package com.trix.man.batch.config;

import com.trix.man.batch.model.User;
import org.springframework.batch.core.ItemWriteListener;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public class UserItemWriteListener implements ItemWriteListener<User> {


    public UserItemWriteListener() {
    }




    public void beforeWrite(List items) {
        System.out.println("Going to write following items: "+ items.toString());
    }

    public void onWriteError(Exception exception, List items) {
        System.out.println("Error occurred when writing items!");

    }
    public void afterWrite(List items) {
    }
}
