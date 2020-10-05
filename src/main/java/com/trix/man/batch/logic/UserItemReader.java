package com.trix.man.batch.logic;

import com.trix.man.batch.model.User;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.beans.factory.annotation.Value;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UserItemReader implements ItemReader<User> {
    private ItemReader<User> delegate;

private String csvFile;

    public UserItemReader(String csvFile) {
        this.csvFile = csvFile;
    }


    @Override
    public User read() throws Exception {
        if (delegate == null) {
            System.out.println("Created the delegate");
            delegate = new IteratorItemReader<>(users());
        }
        return delegate.read();
    }


    private List<User> users() throws IOException {
//        try (XMLDecoder decoder = new XMLDecoder(new FileInputStream(filename))) {
//            return (List<Customer>) decoder.readObject();
//        }
        //Create the CSVFormat object
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');

        //initialize the CSVParser object
        CSVParser parser = new CSVParser(new FileReader(csvFile), format);
        List<User> users = new ArrayList<>();
        for(CSVRecord record : parser){
            User user = new User();
            user.setId(Integer.getInteger(record.get("id")));
            user.setName(record.get("name"));
            user.setDept(record.get("dept"));
            user.setSalary(Integer.getInteger(record.get("salary")));
            users.add(user);
        }
        //close the parser
        parser.close();
      return users;
    }
}
