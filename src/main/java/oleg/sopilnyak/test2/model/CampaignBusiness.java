package oleg.sopilnyak.test2.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Business Model type for Campaign
 */
@Data
@EqualsAndHashCode(exclude = {"segments"})
@ToString
@NoArgsConstructor
public class CampaignBusiness {

    private String name;
    private Set<Integer> segments = new LinkedHashSet<>();
    private Integer popularity = 0;
    public void incrementPopularity(){
        popularity++;
    }
}
