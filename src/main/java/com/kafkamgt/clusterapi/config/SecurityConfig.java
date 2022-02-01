package com.kafkamgt.clusterapi.config;

import org.jasypt.util.text.BasicTextEncryptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;

@EnableWebSecurity
@PropertySource(value= {"classpath:application.properties"})
public class SecurityConfig extends WebSecurityConfigurerAdapter{

    @Value("${kafkawize.clusterapi.access.username}")
    String user1_username;

    @Value("${kafkawize.clusterapi.access.password}")
    String user1_pwd;

    @Value("${kafkawize.jasypt.encryptor.secretkey}")
    String encryptorSecretKey;

    @Autowired
    public void configureGlobal(AuthenticationManagerBuilder auth)  throws Exception {

        PasswordEncoder encoder =
                PasswordEncoderFactories.createDelegatingPasswordEncoder();

        auth.inMemoryAuthentication()
                .passwordEncoder(encoder)
                .withUser(user1_username).password(encoder.encode(decodePwd(user1_pwd))).roles("USER");
    }

    @Override
    protected void configure (HttpSecurity http) throws Exception {
        http.csrf().disable()//.authorizeRequests().anyRequest().permitAll()
//                .and()
                .authorizeRequests()
                .anyRequest().fullyAuthenticated()
                .and()
                .httpBasic();
    }

    private String decodePwd(String pwd){
        if(pwd != null) {
            BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
            textEncryptor.setPasswordCharArray(encryptorSecretKey.toCharArray());

            return textEncryptor.decrypt(pwd);
        }
        return "";
    }
}