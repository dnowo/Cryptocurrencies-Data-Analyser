package pl.rpd.projekt.model;

import lombok.*;

@Builder
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Sample {
    private Long id;
    private String name;
}
