package utils;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class LaunchCSV implements Serializable {

    private String mission_name;
    private Integer mission_year;
}
