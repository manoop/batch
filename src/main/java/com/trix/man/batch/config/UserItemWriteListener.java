package com.trix.man.batch.config;

import com.trix.man.batch.model.User;
import org.springframework.batch.core.ItemWriteListener;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public class UserItemWriteListener implements ItemWriteListener<User> {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public UserItemWriteListener() {
    }

    private String INSERT_QUERY = "insert into user_stat(id, name, dept, salary) values (?,?,?,?)";



    public void beforeWrite(List items) {
        System.out.println("Going to write following items: "+ items.toString());
    }

    public void onWriteError(Exception exception, List items) {
        System.out.println("Error occurred when writing items!");

    }
    public void afterWrite(List items) {
        System.out.println("Feeding the stats table");
        int result = 0;

        for(Object obj: items){
            User user = (User) obj;
            Object[] params = {user.getId(),user.getName(), user.getDept(),user.getSalary()};
            result += jdbcTemplate.update(INSERT_QUERY, params);

        }
        System.out.println("Number of rows inserted: "+ result);
    }
}
