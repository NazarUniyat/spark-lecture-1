package utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SpaceXLaunch implements Serializable {

    private Integer launchYear;
    private String[] customers;
    private Boolean launchSuccess;
}
