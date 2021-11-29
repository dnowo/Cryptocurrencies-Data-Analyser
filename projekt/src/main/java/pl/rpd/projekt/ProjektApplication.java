package pl.rpd.projekt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;

@SpringBootApplication
public class ProjektApplication {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir","D:\\RPD\\hadoop\\hadoop-3.2.1");
		SpringApplication.run(ProjektApplication.class, args);
	}

}
