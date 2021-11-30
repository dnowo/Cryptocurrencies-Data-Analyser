package pl.rpd.projekt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;

@SpringBootApplication
public class ProjektApplication {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Users\\root\\Desktop\\Hadoop");
		SpringApplication.run(ProjektApplication.class, args);
	}

}
