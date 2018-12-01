package com.kafkamgt.clusterapi;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;

@EnableWebSecurity
public class SecurityConfig extends WebSecurityConfigurerAdapter{

    @Value("${securityconfig.user1.username}")
    String user1_username;

    @Value("${securityconfig.user1.pwd}")
    String user1_pwd;

    @Value("${securityconfig.user1.role}")
    String user1_role;

    @Value("${securityconfig.user2.username}")
    String user2_username;

    @Value("${securityconfig.user2.pwd}")
    String user2_pwd;

    @Value("${securityconfig.user2.role}")
    String user2_role;

    @Autowired
    public void configureGlobal(AuthenticationManagerBuilder auth)  throws Exception {

        PasswordEncoder encoder =
                PasswordEncoderFactories.createDelegatingPasswordEncoder();

        auth.inMemoryAuthentication()
                .passwordEncoder(encoder)
                .withUser(user1_username).password(encoder.encode(user1_pwd)).roles(user1_role)
                .and()
                .withUser(user2_username).password(encoder.encode(user2_pwd)).roles(user2_role);
    }

    @Override
    protected void configure (HttpSecurity http) throws Exception {
        http.csrf().disable().authorizeRequests().anyRequest().permitAll()
                .and()
                .authorizeRequests()
                .anyRequest()//.fullyAuthenticated()
                //.and()
                //.formLogin()
                .permitAll();
    }
}