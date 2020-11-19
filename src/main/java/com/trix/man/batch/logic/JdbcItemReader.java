package com.trix.man.batch.logic;

import com.trix.man.batch.model.User;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcItemReader extends JdbcPagingItemReader<User> {

    public JdbcItemReader(DataSource ds, int chunkSize) {
        super();
        setDataSource(ds);
        setPageSize(chunkSize);
        setQueryProvider(queryProvider(ds));
        setRowMapper(rowMapper());
    }
    private RowMapper<User> rowMapper() {
        return new RowMapper<User>() {

            @Override
            public User mapRow(ResultSet rs, int rowNum) throws SQLException {
                User user=new User();

                user.setId(String.valueOf(rs.getLong("id")));
                user.setName(rs.getString("name"));
                user.setDept(rs.getString("dept"));
                user.setSalary(String.valueOf(rs.getLong("salary")));
                return user;
            }
        };
    }

    private PagingQueryProvider queryProvider(DataSource ds) {
        SqlPagingQueryProviderFactoryBean queryProvider=new SqlPagingQueryProviderFactoryBean();
        queryProvider.setDataSource(ds);
        queryProvider.setSelectClause("id, name, dept, salary ");
        queryProvider.setFromClause("taxis.user PARTITION(P0)");

        queryProvider.setSortKey("id");
        queryProvider.setWhereClause("id not in (select id from user_stat)");
        try {
            return queryProvider.getObject();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

}
