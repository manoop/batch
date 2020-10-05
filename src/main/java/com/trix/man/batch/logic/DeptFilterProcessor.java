package com.trix.man.batch.logic;

import com.trix.man.batch.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

import java.util.HashMap;
import java.util.Map;

public class DeptFilterProcessor implements ItemProcessor<User, User> {

    private static final Map<String, String> DEPT = new HashMap<>();

    public DeptFilterProcessor() {
        DEPT.put("001", "PB");
        DEPT.put("002", "WM");
        DEPT.put("003", "PCC");
    }

    @Override
    public User process(User item) throws Exception {
        System.out.println("Process started");
        item.setDept(DEPT.get(item.getDept()));
        return item;
    }
}
