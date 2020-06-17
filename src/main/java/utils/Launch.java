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
public class Launch implements Serializable {

    private Integer launchYear;
    private String missionName;
    private Boolean launchSuccess;
    private String details;
}
