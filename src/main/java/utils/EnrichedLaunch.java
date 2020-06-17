package utils;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
@Setter
public class EnrichedLaunch extends Launch implements Serializable {
    private String veryImportantComment;

    public static EnrichedLaunch of(Launch launch, String veryImportantComment) {
        EnrichedLaunch self = new EnrichedLaunch();
        self.setMissionName(launch.getMissionName());
        self.setDetails(launch.getDetails());
        self.setLaunchSuccess(launch.getLaunchSuccess());
        self.setLaunchYear(launch.getLaunchYear());
        self.setVeryImportantComment(veryImportantComment);
        return self;
    }
}
